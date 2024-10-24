package object

import (
	"custodian/server/object/description"
	"errors"
	"fmt"
	rqlParser "github.com/Q-CIS-DEV/go-rql-parser"
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
	KeyField  *FieldDescription
	FieldList []*FieldDescription
	Type      SelectType
}

func (sf *SelectFields) Exclude(field *FieldDescription) error {
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

func (sf *SelectFields) Include(field *FieldDescription) error {
	if sf.Type == SelectFieldsTypeExclude {
		return errors.New("attempted to exclude field from the node, which already has another included one")
	} else if sf.Type == SelectFieldsTypeInclude {
		sf.FieldList = append(sf.FieldList, field)
	} else if sf.Type == SelectFieldsTypeFull {
		sf.FieldList = []*FieldDescription{sf.KeyField}
		sf.Type = SelectFieldsTypeInclude
		//do nothing if the field is already selected
		for _, selectedField := range sf.FieldList {
			if selectedField.Name == field.Name {
				return nil
			}
		}

		sf.FieldList = append(sf.FieldList, field)
	}
	return nil

}

func NewSelectFields(keyField *FieldDescription, fieldList []*FieldDescription) *SelectFields {
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
	LinkField *FieldDescription
	//KeyField is a field of the target object which LinkField is linking to
	KeyField       *FieldDescription
	Meta           *Meta
	ChildNodes     ChildNodes
	Depth          int
	OnlyLink       bool
	Plural         bool
	Parent         *Node
	MetaList       *MetaList
	Type           NodeType
	SelectFields   SelectFields
	RetrievePolicy *AggregatedRetrievePolicy
}

func (node *Node) keyAsString(recordValues map[string]interface{}, objectMeta *Meta) (string, error) {
	v := recordValues[objectMeta.Key.Name]
	str, err := objectMeta.Key.ValueAsString(v)
	return str, err
}

func (node *Node) keyAsNativeType(recordValues map[string]interface{}, objectMeta *Meta) (interface{}, error) {
	v := recordValues[objectMeta.Key.Name]
	valueAsString, _ := objectMeta.Key.ValueAsString(v)
	castValue, err := objectMeta.Key.ValueFromString(valueAsString)
	return castValue, err
}

func (node *Node) ResolveByRql(sc SearchContext, rqlNode *rqlParser.RqlRootNode) ([]*Record, int, error) {
	var results []*Record
	raw, count, err := sc.processor.GetRql(node, rqlNode, node.SelectFields.FieldList, sc.DbTransaction)

	if err == nil {
		for _, obj := range raw {
			results = append(results, node.FillRecordValues(NewRecord(node.Meta, obj, sc.processor), sc))
		}
	}

	return results, count, err
}

func (node *Node) Resolve(sc SearchContext, key interface{}) (*Record, error) {
	var fields []*FieldDescription = nil
	var objectMeta *Meta
	var pkValue interface{}

	switch key.(type) {
	case *Record:
		pkValue = key.(*Record).Pk()
	case *GenericInnerLink:
		pkValue = key.(*GenericInnerLink).Pk
	default:
		pkValue = key
	}

	switch {
	case node.IsOfRegularType():
		objectMeta = node.Meta
	case node.IsOfGenericType():
		if node.Plural {
			objectMeta = node.Meta
		} else {
			switch key.(type) {
			case *Record:
				objectMeta = node.MetaList.GetByName(key.(*Record).Meta.Name)
			case *GenericInnerLink:
				objectMeta = node.MetaList.GetByName(key.(*GenericInnerLink).ObjectName)
			}

				if pkValue == "" {
					return nil, nil
				}
			}
	}

	if node.OnlyLink {
		fields = []*FieldDescription{objectMeta.Key}
	} else {
		fields = node.SelectFields.FieldList
	}

	obj, err := sc.processor.GetSystem(objectMeta, fields, objectMeta.Key.Name, pkValue, sc.DbTransaction)
	if err != nil || obj == nil {
		return nil, err
	}
	//return full record
	if node.IsOfGenericType() {
		obj[GenericInnerLinkObjectKey] = objectMeta.Name
	}

	return node.FillRecordValues(NewRecord(objectMeta, obj, sc.processor), sc), nil

	//return obj, nil
}

func (node *Node) ResolveRegularPlural(sc SearchContext, key interface{}) ([]interface{}, error) {
	// logger.Debug("Resolving plural: node [meta=%s, depth=%s, plural=%s], sc=%s, key=%s", node.Meta.Name, node.Depth, node.plural, sc, key)
	var fields []*FieldDescription = nil
	if node.OnlyLink {
		fields = []*FieldDescription{node.Meta.Key}
	} else {
		fields = node.SelectFields.FieldList
	}
	if records, err := sc.processor.GetAll(node.Meta, fields, map[string]interface{}{node.KeyField.Name: key}, sc.DbTransaction); err != nil {
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
				result[i] = NewRecord(node.Meta, obj, sc.processor)
			}
		}
		return result, nil
	}
}

//Resolve records referenced by generic outer field
func (node *Node) ResolveGenericPlural(sc SearchContext, key interface{}, objectMeta *Meta) ([]interface{}, error) {
	// logger.Debug("Resolving generic plural: node [meta=%s, depth=%s, plural=%s], sc=%s, key=%s", node.Meta.Name, node.Depth, node.plural, sc, key)
	var fields []*FieldDescription = nil
	if node.OnlyLink {
		fields = []*FieldDescription{node.Meta.Key}
	}
	if records, err := sc.processor.GetAll(node.Meta, fields, map[string]interface{}{
		GetGenericFieldKeyColumnName(node.KeyField.Name):  key,
		GetGenericFieldTypeColumnName(node.KeyField.Name): objectMeta.Name,
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
				result[i] = NewRecord(objectMeta, obj, sc.processor)
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
		fields := []*FieldDescription{node.KeyField}
		if records, err := sc.processor.GetAll(node.Meta, fields, map[string]interface{}{linkField.Name: key}, sc.DbTransaction); err != nil {
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
		searchContext := SearchContext{DepthLimit: 1, processor: sc.processor, LazyPath: "/custodian/data/bulk", DbTransaction: sc.DbTransaction, OmitOuters: sc.OmitOuters}
		root := &Node{
			KeyField:       node.LinkField.LinkMeta.Key,
			Meta:           node.LinkField.LinkMeta,
			ChildNodes:     *NewChildNodes(),
			Depth:          1,
			OnlyLink:       false,
			Plural:         false,
			Parent:         nil,
			Type:           NodeTypeRegular,
			SelectFields:   *NewSelectFields(node.LinkField.LinkMeta.Key, node.LinkField.LinkMeta.TableFields()),
			RetrievePolicy: node.RetrievePolicy,
		}
		root.RecursivelyFillChildNodes(searchContext.DepthLimit, description.FieldModeRetrieve)

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

func (node *Node) fillDirectChildNodes(depthLimit int, fieldMode description.FieldMode) {
	//process regular links, skip generic child nodes
	onlyLink := false
	if node.Depth >= depthLimit {
		onlyLink = true
	}
	if node.Meta != nil {
		for i := range node.Meta.Fields {
			node.FillChildNode(&node.Meta.Fields[i], onlyLink, fieldMode, node.RetrievePolicy.SubPolicyForNode(node.Meta.Fields[i].Name), SelectFieldsTypeFull)
		}
	}
}

func (node *Node) FillChildNode(fieldDescription *FieldDescription, onlyLink bool, fieldMode description.FieldMode, policy *AggregatedRetrievePolicy, selectType SelectType) *Node {
	//process regular links, skip generic child nodes

	//skip outer link which does not have retrieve mode set to true
	if fieldDescription.LinkType == description.LinkTypeOuter && !fieldDescription.RetrieveMode && fieldMode != description.FieldModeQuery {
		return nil
	}
	childNodes := *NewChildNodes()
	if selectType == SelectFieldsTypeInclude {
		//return existing node if no node exist create it
		if len(node.ChildNodes.nodes) > 0 {
			if childNode, ok := node.ChildNodes.Get(fieldDescription.Name); ok {
				return childNode
			}
		}
	}
	if fieldDescription.Type == description.FieldTypeObject && fieldDescription.LinkType == description.LinkTypeInner && (node.Parent == nil || !IsBackLink(node.Parent.Meta, fieldDescription)) {
		node.ChildNodes.Set(fieldDescription.Name, &Node{
			LinkField:      fieldDescription,
			KeyField:       fieldDescription.LinkMeta.Key,
			Meta:           fieldDescription.LinkMeta,
			ChildNodes:     childNodes,
			Depth:          node.Depth + 1,
			OnlyLink:       onlyLink,
			Plural:         false,
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
			Plural:         true,
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
			Plural:         true,
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
				Plural:         false,
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
				Plural:         true,
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
				if nodesToProcess[0].IsOfRegularType() || (nodesToProcess[0].IsOfGenericType() && nodesToProcess[0].Plural) {
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
		Plural:       node.Plural,
		Parent:       node.Parent,
		MetaList:     node.MetaList,
		Type:         node.Type,
		SelectFields: SelectFields{FieldList: append([]*FieldDescription(nil), node.SelectFields.FieldList...), Type: node.SelectFields.Type},
	}
}
