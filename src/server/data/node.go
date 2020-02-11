package data

import (
	"errors"
	"fmt"
	"github.com/Q-CIS-DEV/go-rql-parser"
	"logger"
	. "server/data/record"
	"server/data/types"
	"server/object/meta"
	"strings"
)

type NodeType int

const (
	NodeTypeRegular NodeType = iota + 1
	NodeTypeGeneric
)

//

type SelectType int

const (
	SelectFieldsTypeFull    SelectType = iota + 1
	SelectFieldsTypeExclude
	SelectFieldsTypeInclude
)

type SelectFields struct {
	KeyField  *meta.Field
	FieldList []*meta.Field
	Type      SelectType
}

func (sf *SelectFields) Exclude(field *meta.Field) error {
	if sf.Type == SelectFieldsTypeInclude {
		return errors.New("attempted to exclude field from the node, which already has another included one")
	}
	for i := range sf.FieldList {
		if sf.FieldList[i].Name == field.Name {
			if sf.KeyField.Name != field.Name {
				sf.FieldList = append(sf.FieldList[:i], sf.FieldList[i+1:]...)
				break
			}
		}
	}
	sf.Type = SelectFieldsTypeExclude
	return nil
}

func (sf *SelectFields) Include(field *meta.Field) error {
	if sf.Type == SelectFieldsTypeExclude {
		return errors.New("attempted to exclude field from the node, which already has another included one")
	} else if sf.Type == SelectFieldsTypeFull {
		sf.FieldList = []*meta.Field{sf.KeyField}
	}

	sf.Type = SelectFieldsTypeInclude
	//do nothing if the field is already selected
	for _, selectedField := range sf.FieldList {
		if selectedField.Name == field.Name {
			return nil
		}
	}

	sf.FieldList = append(sf.FieldList, field)
	return nil
}

func NewSelectFields(keyField *meta.Field, fieldList []*meta.Field) *SelectFields {
	return &SelectFields{KeyField: keyField, FieldList: fieldList, Type: SelectFieldsTypeFull}
}

//

type ChildNodes struct {
	nodes      map[string]*Node
	selectType SelectType
}

func (cn *ChildNodes) Exclude(name string) error {
	if cn.selectType == SelectFieldsTypeInclude {
		return errors.New("attempted to exclude field from the node, which already has another included one")
	}

	if _, ok := cn.nodes[name]; ok {
		delete(cn.nodes, name)
	}
	cn.selectType = SelectFieldsTypeExclude
	return nil
}

func (cn *ChildNodes) Empty() {
	if cn.selectType == SelectFieldsTypeFull {
		cn.nodes = make(map[string]*Node)
		cn.selectType = SelectFieldsTypeInclude
	}
}

func (cn *ChildNodes) Set(name string, node *Node) {
	cn.nodes[name] = node
}
func (cn *ChildNodes) Get(name string) (*Node, bool) {
	node, ok := cn.nodes[name]
	return node, ok
}

func (cn *ChildNodes) Nodes() map[string]*Node {
	return cn.nodes
}

func NewChildNodes() *ChildNodes {
	return &ChildNodes{nodes: make(map[string]*Node), selectType: SelectFieldsTypeFull}
}

type Node struct {
	//LinkField is a field which links to the target object
	LinkField *meta.Field
	//KeyField is a field of the target object which LinkField is linking to
	KeyField       *meta.Field
	Meta           *meta.Meta
	ChildNodes     ChildNodes
	Depth          int
	OnlyLink       bool
	plural         bool
	Parent         *Node
	MetaList       *meta.MetaList
	Type           NodeType
	SelectFields   SelectFields
	RetrievePolicy *AggregatedRetrievePolicy
}

func (node *Node) keyAsString(recordValues map[string]interface{}, objectMeta *meta.Meta) (string, error) {
	v := recordValues[objectMeta.Key]
	str, err := objectMeta.GetKey().ValueAsString(v)
	return str, err
}

func (node *Node) keyAsNativeType(recordValues map[string]interface{}, objectMeta *meta.Meta) (interface{}, error) {
	v := recordValues[objectMeta.Key]
	valueAsString, _ := objectMeta.GetKey().ValueAsString(v)
	castValue, err := objectMeta.GetKey().ValueFromString(valueAsString)
	return castValue, err
}

func (node *Node) ResolveByRql(sc SearchContext, rqlNode *rqlParser.RqlRootNode) ([]*Record, int, error) {
	var results []*Record
	raw, count, err := sc.dm.GetRql(node, rqlNode, node.SelectFields.FieldList, sc.DbTransaction)

	if err == nil {
		for _, obj := range raw  {
			results = append(results, node.FillRecordValues(NewRecord(node.Meta, obj), sc))
		}
	}

	return results, count, err
}

func (node *Node) Resolve(sc SearchContext, key interface{}) (*Record, error) {
	var fields []*meta.Field = nil
	var objectMeta *meta.Meta
	var pkValue interface{}

	switch key.(type) {
	case *Record:
		pkValue = key.(*Record).Pk()
	case *types.GenericInnerLink:
		pkValue = key.(*types.GenericInnerLink).Pk
	default:
		pkValue = key
	}

	switch {
	case node.IsOfRegularType():
		objectMeta = node.Meta
	case node.IsOfGenericType():
		if node.plural {
			objectMeta = node.Meta
		} else {
			switch key.(type) {
			case *Record:
				objectMeta = node.MetaList.GetByName(key.(*Record).Meta.Name)
			case *types.GenericInnerLink:
				objectMeta = node.MetaList.GetByName(key.(*types.GenericInnerLink).ObjectName)
			}

			if pkValue == "" {
				return nil, nil
			}
		}

	}

	if node.OnlyLink {
		fields = []*meta.Field{objectMeta.GetKey()}
	} else {
		fields = node.SelectFields.FieldList
	}

	obj, err := sc.dm.Get(objectMeta, fields, objectMeta.Key, pkValue, sc.DbTransaction)
	if err != nil || obj == nil {
		return nil, err
	}
	//return full record
	if node.IsOfGenericType() {
		obj[types.GenericInnerLinkObjectKey] = objectMeta.Name
	}

	return node.FillRecordValues(NewRecord(objectMeta, obj), sc), nil

	//return obj, nil
}

func (node *Node) ResolveRegularPlural(sc SearchContext, key interface{}) ([]interface{}, error) {
	logger.Debug("Resolving plural: node [meta=%s, depth=%s, plural=%s], sc=%s, key=%s", node.Meta.Name, node.Depth, node.plural, sc, key)
	var fields []*meta.Field = nil
	if node.OnlyLink {
		fields = []*meta.Field{node.Meta.GetKey()}
	} else {
		fields = node.SelectFields.FieldList
	}
	if records, err := sc.dm.GetAll(node.Meta, fields, map[string]interface{}{node.KeyField.Name: key}, sc.DbTransaction); err != nil {
		return nil, err
	} else {
		result := make([]interface{}, len(records), len(records))
		if node.OnlyLink {
			for i, record := range records {
				if keyValue, err := node.keyAsNativeType(record, node.Meta); err != nil {
					return nil, err
				} else {
					result[i] = keyValue
				}
			}
		} else {
			for i, obj := range records {
				result[i] = NewRecord(node.Meta, obj)
			}
		}
		return result, nil
	}
}

//Resolve records referenced by generic outer field
func (node *Node) ResolveGenericPlural(sc SearchContext, key interface{}, objectMeta *meta.Meta) ([]interface{}, error) {
	logger.Debug("Resolving generic plural: node [meta=%s, depth=%s, plural=%s], sc=%s, key=%s", node.Meta.Name, node.Depth, node.plural, sc, key)
	var fields []*meta.Field = nil
	if node.OnlyLink {
		fields = []*meta.Field{node.Meta.GetKey()}
	}
	if records, err := sc.dm.GetAll(node.Meta, fields, map[string]interface{}{
		meta.GetGenericFieldKeyColumnName(node.KeyField.Name):  key,
		meta.GetGenericFieldTypeColumnName(node.KeyField.Name): objectMeta.Name,
	}, sc.DbTransaction); err != nil {
		return nil, err
	} else {
		result := make([]interface{}, len(records), len(records))
		if node.OnlyLink {
			for i, obj := range records {
				keyValue, err := node.keyAsNativeType(obj, node.Meta)
				if err != nil {
					return nil, err
				}
				result[i] = keyValue
			}
		} else {
			for i, obj := range records {
				result[i] = NewRecord(objectMeta, obj)
			}
		}
		return result, nil
	}
}

func (node *Node) ResolvePluralObjects(sc SearchContext, key interface{}) ([]interface{}, error) {
	if node.OnlyLink {
		//get a field which points to parent object
		linkField := node.Meta.FindField(node.LinkField.Meta.Name)
		//specify field, which value should be retrieved
		fields := []*meta.Field{node.KeyField}
		if records, err := sc.dm.GetAll(node.Meta, fields, map[string]interface{}{linkField.Name: key}, sc.DbTransaction); err != nil {
			return nil, err
		} else {
			result := make([]interface{}, len(records), len(records))
			for i, data := range records {
				result[i] = data[node.KeyField.Name]
			}
			return result, nil
		}
	} else {
		keyStr, _ := node.LinkField.Meta.GetKey().ValueAsString(key)
		//query records of LinkMeta by LinkThrough`s field
		//Eg: for record of object A with ID 56 which has "Objects" relation to B called "bs" filter will look like this:
		//eq(b__s_set.a,56)
		//and querying is performed by B meta
		filter := fmt.Sprintf("eq(%s.%s,%s)", node.LinkField.LinkThrough.FindField(node.LinkField.LinkMeta.Name).ReverseOuterField().Name, node.LinkField.Meta.Name, keyStr)
		searchContext := SearchContext{depthLimit: 1, dm: sc.dm, lazyPath: "/custodian/data/bulk", DbTransaction: sc.DbTransaction, omitOuters: sc.omitOuters}
		root := &Node{
			KeyField:       node.LinkField.LinkMeta.GetKey(),
			Meta:           node.LinkField.LinkMeta,
			ChildNodes:     *NewChildNodes(),
			Depth:          1,
			OnlyLink:       false,
			plural:         false,
			Parent:         nil,
			Type:           NodeTypeRegular,
			SelectFields:   *NewSelectFields(node.LinkField.LinkMeta.GetKey(), node.LinkField.LinkMeta.TableFields()),
			RetrievePolicy: node.RetrievePolicy,
		}
		root.RecursivelyFillChildNodes(searchContext.depthLimit, meta.FieldModeRetrieve)

		parser := rqlParser.NewParser()
		rqlNode, err := parser.Parse(strings.NewReader(filter))
		if err != nil {
			return nil, err
		}
		records, _, err := root.ResolveByRql(searchContext, rqlNode)

		result := make([]interface{}, len(records), len(records))
		for i, data := range records {
			result[i] = data
		}
		return result, nil
	}
}

func (node *Node) fillDirectChildNodes(depthLimit int, fieldMode meta.FieldMode) {
	//process regular links, skip generic child nodes
	onlyLink := false
	if node.Depth >= depthLimit {
		onlyLink = true
	}
	if node.Meta != nil {
		for i := range node.Meta.Fields {
			node.FillChildNode(node.Meta.Fields[i], onlyLink, fieldMode, node.RetrievePolicy.SubPolicyForNode(node.Meta.Fields[i].Name))
		}
	}
}

func (node *Node) FillChildNode(fieldDescription *meta.Field, onlyLink bool, fieldMode meta.FieldMode, policy *AggregatedRetrievePolicy) *Node {
	//process regular links, skip generic child nodes

	//skip outer link which does not have retrieve mode set to true
	if fieldDescription.LinkType == meta.LinkTypeOuter && !fieldDescription.RetrieveMode && fieldMode != meta.FieldModeQuery {
		return nil
	}
	childNodes := *NewChildNodes()

	if fieldDescription.Type == meta.FieldTypeObject && fieldDescription.LinkType == meta.LinkTypeInner && (node.Parent == nil || !isBackLink(node.Parent.Meta, fieldDescription)) {
		node.ChildNodes.Set(fieldDescription.Name, &Node{
			LinkField:      fieldDescription,
			KeyField:       fieldDescription.LinkMeta.GetKey(),
			Meta:           fieldDescription.LinkMeta,
			ChildNodes:     childNodes,
			Depth:          node.Depth + 1,
			OnlyLink:       onlyLink,
			plural:         false,
			Parent:         node,
			Type:           NodeTypeRegular,
			SelectFields:   *NewSelectFields(fieldDescription.LinkMeta.GetKey(), fieldDescription.LinkMeta.TableFields()),
			RetrievePolicy: policy,
		})
	} else if fieldDescription.Type == meta.FieldTypeArray && fieldDescription.LinkType == meta.LinkTypeOuter {
		node.ChildNodes.Set(fieldDescription.Name, &Node{
			LinkField:      fieldDescription,
			KeyField:       fieldDescription.OuterLinkField,
			Meta:           fieldDescription.LinkMeta,
			ChildNodes:     childNodes,
			Depth:          node.Depth + 1,
			OnlyLink:       onlyLink,
			plural:         true,
			Parent:         node,
			Type:           NodeTypeRegular,
			SelectFields:   *NewSelectFields(fieldDescription.LinkMeta.GetKey(), fieldDescription.LinkMeta.TableFields()),
			RetrievePolicy: policy,
		})
	} else if fieldDescription.Type == meta.FieldTypeObjects {
		node.ChildNodes.Set(fieldDescription.Name, &Node{
			LinkField:      fieldDescription,
			KeyField:       fieldDescription.LinkThrough.FindField(fieldDescription.LinkMeta.Name),
			Meta:           fieldDescription.LinkThrough,
			ChildNodes:     childNodes,
			Depth:          node.Depth + 1,
			OnlyLink:       onlyLink,
			plural:         true,
			Parent:         node,
			Type:           NodeTypeRegular,
			SelectFields:   *NewSelectFields(fieldDescription.LinkMeta.GetKey(), fieldDescription.LinkMeta.TableFields()),
			RetrievePolicy: policy,
		})
	} else if fieldDescription.Type == meta.FieldTypeGeneric {
		if fieldDescription.LinkType == meta.LinkTypeInner {
			node.ChildNodes.Set(fieldDescription.Name, &Node{
				LinkField:      fieldDescription,
				KeyField:       nil,
				Meta:           nil,
				ChildNodes:     childNodes,
				Depth:          node.Depth + 1,
				OnlyLink:       onlyLink,
				plural:         false,
				Parent:         node,
				MetaList:       &meta.MetaList{fieldDescription.LinkMetaList},
				Type:           NodeTypeGeneric,
				RetrievePolicy: policy,
			})

		} else if fieldDescription.LinkType == meta.LinkTypeOuter {
			node.ChildNodes.Set(fieldDescription.Name, &Node{
				LinkField:      fieldDescription,
				KeyField:       fieldDescription.OuterLinkField,
				Meta:           fieldDescription.LinkMeta,
				ChildNodes:     childNodes,
				Depth:          node.Depth + 1,
				OnlyLink:       onlyLink,
				plural:         true,
				Parent:         node,
				Type:           NodeTypeGeneric,
				RetrievePolicy: policy,
			})
		}
	}
	if childNode, ok := node.ChildNodes.Get(fieldDescription.Name); !ok {
		return nil
	} else {
		return childNode
	}
}

func (node *Node) RecursivelyFillChildNodes(depthLimit int, fieldMode meta.FieldMode) error {
	node.fillDirectChildNodes(depthLimit, fieldMode)
	if !node.IsOfGenericType() {
		nodesToProcess := make([]*Node, 0)
		for _, v := range node.ChildNodes.Nodes() {
			nodesToProcess = append(nodesToProcess, v)
		}
		processedNodesNames := map[string]bool{node.Meta.Name: true}
		//recursively fill all dependent nodes beginning from root node
		for ; len(nodesToProcess) > 0; nodesToProcess = nodesToProcess[1:] {
			if !nodesToProcess[0].OnlyLink {
				if nodesToProcess[0].IsOfRegularType() || (nodesToProcess[0].IsOfGenericType() && nodesToProcess[0].plural) {
					nodesToProcess[0].fillDirectChildNodes(depthLimit, fieldMode)
					processedNodesNames[nodesToProcess[0].Meta.Name] = true
					for _, childNode := range nodesToProcess[0].ChildNodes.Nodes() {
						//generic fields` meta could not be resolved without fields value
						if childNode.IsOfRegularType() && !processedNodesNames[childNode.Meta.Name] || childNode.IsOfGenericType() {
							nodesToProcess = append(nodesToProcess, childNode)
						}
					}
				}
			}
		}
	}
	return node.RetrievePolicy.Apply(node)
}

func (node *Node) FillRecordValues(record *Record, searchContext SearchContext) *Record {
	nodeCopy := node
	//node may mutate during resolving of generic fields, thus local copy of node is required
	for nodeResults := []ResultNode{{nodeCopy, record}}; len(nodeResults) > 0; nodeResults = nodeResults[1:] {
		childNodesResults, _ := nodeResults[0].getFilledChildNodes(searchContext)
		nodeResults = append(nodeResults, childNodesResults...)
	}
	return record
}

//traverse values and prepare them for output
func transformValues(values map[string]interface{}) map[string]interface{} {
	for key, value := range values {
		switch  castValue := value.(type) {
		case map[string]interface{}:
			values[key] = transformValues(castValue)
		case []interface{}:
			for i := range castValue {
				switch castValueItem := castValue[i].(type) {
				case map[string]interface{}:
					castValue[i] = transformValues(castValueItem)
				}
			}
		case *types.GenericInnerLink:
			values[key] = castValue.AsMap()
		}
	}
	return values
}

func (node *Node) IsOfGenericType() bool {
	return node.Type == NodeTypeGeneric
}

func (node *Node) IsOfRegularType() bool {
	return node.Type == NodeTypeRegular
}

func (node *Node) Clone() *Node {
	return &Node{
		LinkField:    node.LinkField,
		Meta:         node.Meta,
		ChildNodes:   *NewChildNodes(),
		Depth:        node.Depth,
		OnlyLink:     node.OnlyLink,
		plural:       node.plural,
		Parent:       node.Parent,
		MetaList:     node.MetaList,
		Type:         node.Type,
		SelectFields: SelectFields{FieldList: append([]*meta.Field(nil), node.SelectFields.FieldList...), Type: node.SelectFields.Type},
	}
}
