package data

import (
	. "custodian/server/data/record"
	"custodian/server/data/types"
	"custodian/server/object/description"
	"custodian/server/object/meta"
	"errors"
	"fmt"
	rqlParser "github.com/Q-CIS-DEV/go-rql-parser"
	"reflect"
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
	SelectFieldsTypeFull SelectType = iota + 1
	SelectFieldsTypeExclude
	SelectFieldsTypeInclude
)

type SelectFields struct {
	KeyField  *meta.FieldDescription
	FieldList []*meta.FieldDescription
	Type      SelectType
}

func (sf *SelectFields) Exclude(field *meta.FieldDescription) error {
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

func (sf *SelectFields) Include(field *meta.FieldDescription) error {
	if sf.Type == SelectFieldsTypeExclude {
		return errors.New("attempted to exclude field from the node, which already has another included one")
	} else if sf.Type == SelectFieldsTypeFull {
		sf.FieldList = []*meta.FieldDescription{sf.KeyField}
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

func NewSelectFields(keyField *meta.FieldDescription, fieldList []*meta.FieldDescription) *SelectFields {
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
	LinkField *meta.FieldDescription
	//KeyField is a field of the target object which LinkField is linking to
	KeyField       *meta.FieldDescription
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
	v := recordValues[objectMeta.Key.Name]
	str, err := objectMeta.Key.ValueAsString(v)
	return str, err
}

func (node *Node) keyAsNativeType(recordValues map[string]interface{}, objectMeta *meta.Meta) (interface{}, error) {
	v := recordValues[objectMeta.Key.Name]
	valueAsString, _ := objectMeta.Key.ValueAsString(v)
	castValue, err := objectMeta.Key.ValueFromString(valueAsString)
	return castValue, err
}

func (node *Node) ResolveByRql(sc SearchContext, rqlNode *rqlParser.RqlRootNode) ([]*Record, int, error) {
	var results []*Record
	raw, count, err := sc.dm.GetRql(node, rqlNode, node.SelectFields.FieldList, sc.DbTransaction)

	if err == nil {
		for _, obj := range raw {
			results = append(results, node.FillRecordValues(NewRecord(node.Meta, obj), sc))
		}
	}

	return results, count, err
}

func (node *Node) Resolve(sc SearchContext, key interface{}) (*Record, error) {
	var fields []*meta.FieldDescription = nil
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
		fields = []*meta.FieldDescription{objectMeta.Key}
	} else {
		fields = node.SelectFields.FieldList
	}

	obj, err := sc.dm.Get(objectMeta, fields, objectMeta.Key.Name, pkValue, sc.DbTransaction)
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
	// logger.Debug("Resolving plural: node [meta=%s, depth=%s, plural=%s], sc=%s, key=%s", node.Meta.Name, node.Depth, node.plural, sc, key)
	var fields []*meta.FieldDescription = nil
	if node.OnlyLink {
		fields = []*meta.FieldDescription{node.Meta.Key}
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
	// logger.Debug("Resolving generic plural: node [meta=%s, depth=%s, plural=%s], sc=%s, key=%s", node.Meta.Name, node.Depth, node.plural, sc, key)
	var fields []*meta.FieldDescription = nil
	if node.OnlyLink {
		fields = []*meta.FieldDescription{node.Meta.Key}
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
		fields := []*meta.FieldDescription{node.KeyField}
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
		keyStr, _ := node.LinkField.Meta.Key.ValueAsString(key)
		//query records of LinkMeta by LinkThrough`s field
		//Eg: for record of object A with ID 56 which has "Objects" relation to B called "bs" filter will look like this:
		//eq(b__s_set.a,56)
		//and querying is performed by B meta
		filter := fmt.Sprintf("eq(%s.%s,%s)", node.LinkField.LinkThrough.FindField(node.LinkField.LinkMeta.Name).ReverseOuterField().Name, node.LinkField.Meta.Name, keyStr)
		searchContext := SearchContext{depthLimit: 1, dm: sc.dm, lazyPath: "/custodian/data/bulk", DbTransaction: sc.DbTransaction, omitOuters: sc.omitOuters}
		root := &Node{
			KeyField:       node.LinkField.LinkMeta.Key,
			Meta:           node.LinkField.LinkMeta,
			ChildNodes:     *NewChildNodes(),
			Depth:          1,
			OnlyLink:       false,
			plural:         false,
			Parent:         nil,
			Type:           NodeTypeRegular,
			SelectFields:   *NewSelectFields(node.LinkField.LinkMeta.Key, node.LinkField.LinkMeta.TableFields()),
			RetrievePolicy: node.RetrievePolicy,
		}
		root.RecursivelyFillChildNodes(searchContext.depthLimit, description.FieldModeRetrieve)

		parser := rqlParser.NewParser()
		rqlNode, err := parser.Parse(filter)
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

func contains(s []*meta.FieldDescription, e string) (int, bool) {
	for i, a := range s {
		if a.Name == e {
			return i, true
		}
	}
	return -1, false
}

// TODO This is the func that we need to refactor. Add support for exclude and only

func (node *Node) fillDirectChildNodes(depthLimit int, fieldMode description.FieldMode, rp *AggregatedRetrievePolicy) {
	//process regular links, skip generic child nodes
	fmt.Println(rp)
	onlyLink := false
	if node.Depth >= depthLimit {
			onlyLink = true
	}
	if node.Meta != nil {
		if node.RetrievePolicy == nil || len(node.RetrievePolicy.childPolicies) == 0 {
			for i := range node.Meta.Fields {
				node.FillChildNode(&node.Meta.Fields[i], onlyLink, fieldMode, node.RetrievePolicy.SubPolicyForNode(node.Meta.Fields[i].Name))
			}
		} else {

			for i := range node.Meta.Fields {
				node.FillChildNode(&node.Meta.Fields[i], onlyLink, fieldMode, node.RetrievePolicy.SubPolicyForNode(node.Meta.Fields[i].Name))
			}
			//node.handleIncludeExclude(onlyLink, fieldMode)
		}
	}
}

func (node *Node) handleIncludeExclude (onlyLink bool, fieldMode description.FieldMode) {
	keyField := node.SelectFields.KeyField
	node.SelectFields.FieldList = []*meta.FieldDescription{keyField}
	for i := range node.Meta.Fields {
		fieldName := node.Meta.Fields[i].Name
		for _, pathItem := range node.RetrievePolicy.childPolicies {
			if len(pathItem.PathItems()) == 1 {
				if pathItem.PathItems()[0] == fieldName {
					if reflect.TypeOf(pathItem).String() == "*data.includeNodeRetrievePolicy" {
						// Handle include here.
						node.addField(fieldName, i, onlyLink, fieldMode)
					} else if reflect.TypeOf(pathItem).String() == "*data.excludeNodeRetrievePolicy" {
						//	Handle exclude here. Delete field if it was added previously
						fmt.Println("Handle exclude field")
						node.deleteField(fieldName)
					}
				} else {
					fmt.Println("Do not include", fieldName)
					//  Do not include field
					if node.Meta.Fields[i].RetrieveMode == true  && reflect.TypeOf(pathItem).String() == "*data.excludeNodeRetrievePolicy" {
						node.FillChildNode(&node.Meta.Fields[i], onlyLink, fieldMode, node.RetrievePolicy.SubPolicyForNode(node.Meta.Fields[i].Name))
					}
				}
			} else {
				if pathItem.PathItems()[0] == fieldName {
					onlyLink = false
				}
				node.addField(fieldName, i, onlyLink, fieldMode)
			}
		}
	}
}

func (node *Node) addField(fieldName string, i int, onlyLink bool, fieldMode description.FieldMode) {
	_, isIn := contains(node.SelectFields.FieldList, fieldName)
	//if !contains(node.SelectFields.FieldList, fieldName) {
	if !isIn {
		if !strings.HasSuffix(fieldName, "_set") {
			node.SelectFields.FieldList = append(node.SelectFields.FieldList, &node.Meta.Fields[i])
		}
		node.FillChildNode(&node.Meta.Fields[i], onlyLink, fieldMode, node.RetrievePolicy.SubPolicyForNode(node.Meta.Fields[i].Name))
	}
}

func (node *Node) deleteField(fieldName string) {
	i, isIn := contains(node.SelectFields.FieldList, fieldName)
	if isIn {
		fl := node.SelectFields.FieldList
		node.SelectFields.FieldList = append(fl[:i], fl[i+1:]...)
	}
}

func (node *Node) FillChildNode(fieldDescription *meta.FieldDescription, onlyLink bool, fieldMode description.FieldMode, policy *AggregatedRetrievePolicy) *Node {
	//process regular links, skip generic child nodes

	//skip outer link which does not have retrieve mode set to true
	if fieldDescription.LinkType == description.LinkTypeOuter && !fieldDescription.RetrieveMode && fieldMode != description.FieldModeQuery {
		return nil
	}
	childNodes := *NewChildNodes()

	if fieldDescription.Type == description.FieldTypeObject && fieldDescription.LinkType == description.LinkTypeInner && (node.Parent == nil || !isBackLink(node.Parent.Meta, fieldDescription)) {
		node.ChildNodes.Set(fieldDescription.Name, &Node{
			LinkField:      fieldDescription,
			KeyField:       fieldDescription.LinkMeta.Key,
			Meta:           fieldDescription.LinkMeta,
			ChildNodes:     childNodes,
			Depth:          node.Depth + 1,
			OnlyLink:       onlyLink,
			plural:         false,
			Parent:         node,
			Type:           NodeTypeRegular,
			SelectFields:   *NewSelectFields(fieldDescription.LinkMeta.Key, fieldDescription.LinkMeta.TableFields()),
			RetrievePolicy: policy,
		})
	} else if fieldDescription.Type == description.FieldTypeArray && fieldDescription.LinkType == description.LinkTypeOuter {
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
			SelectFields:   *NewSelectFields(fieldDescription.LinkMeta.Key, fieldDescription.LinkMeta.TableFields()),
			RetrievePolicy: policy,
		})
	} else if fieldDescription.Type == description.FieldTypeObjects {
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
			SelectFields:   *NewSelectFields(fieldDescription.LinkMeta.Key, fieldDescription.LinkMeta.TableFields()),
			RetrievePolicy: policy,
		})
	} else if fieldDescription.Type == description.FieldTypeGeneric {
		if fieldDescription.LinkType == description.LinkTypeInner {
			node.ChildNodes.Set(fieldDescription.Name, &Node{
				LinkField:      fieldDescription,
				KeyField:       nil,
				Meta:           nil,
				ChildNodes:     childNodes,
				Depth:          node.Depth + 1,
				OnlyLink:       onlyLink,
				plural:         false,
				Parent:         node,
				MetaList:       fieldDescription.LinkMetaList,
				Type:           NodeTypeGeneric,
				RetrievePolicy: policy,
			})

		} else if fieldDescription.LinkType == description.LinkTypeOuter {
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

func (node *Node) RecursivelyFillChildNodes(depthLimit int, fieldMode description.FieldMode) error {
	node.fillDirectChildNodes(depthLimit, fieldMode, node.RetrievePolicy)
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
					// TODO: This is where we fill in child nodes and maybe here we should also take care of only and exclude
					nodesToProcess[0].fillDirectChildNodes(depthLimit, fieldMode, node.RetrievePolicy)
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
	fmt.Println("HETRETE")
	node.handleIncludeExclude(false, fieldMode)
	//return node.RetrievePolicy.Apply(node)
	return nil
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
		switch castValue := value.(type) {
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
		SelectFields: SelectFields{FieldList: append([]*meta.FieldDescription(nil), node.SelectFields.FieldList...), Type: node.SelectFields.Type},
	}
}
