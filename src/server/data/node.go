package data

import (
	"logger"
	"server/object/meta"
	"github.com/Q-CIS-DEV/go-rql-parser"
	"server/data/types"
	"server/object/description"
)

type NodeType int

const (
	NodeTypeRegular NodeType = iota + 1
	NodeTypeGeneric
)

type Node struct {
	//LinkField is a field which links to the target object
	LinkField *meta.FieldDescription
	//KeyField is a field of the target object which LinkField is linking to
	KeyField   *meta.FieldDescription
	Meta       *meta.Meta
	ChildNodes map[string]*Node
	Depth      int
	OnlyLink   bool
	plural     bool
	Parent     *Node
	MetaList   *meta.MetaList
	Type       NodeType
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

func (node *Node) ResolveByRql(sc SearchContext, rqlNode *rqlParser.RqlRootNode) ([]map[string]interface{}, int, error) {
	return sc.dm.GetRql(node, rqlNode, nil, sc.DbTransaction)
}

func (node *Node) Resolve(sc SearchContext, key interface{}) (interface{}, error) {
	var fields []*meta.FieldDescription = nil
	var objectMeta *meta.Meta
	var pkValue interface{}

	switch {
	case node.IsOfRegularType():
		objectMeta = node.Meta
		pkValue = key
	case node.IsOfGenericType():
		if node.plural {
			objectMeta = node.Meta
			pkValue = key
		} else {
			objectMeta = node.MetaList.GetByName(key.(types.GenericInnerLink).ObjectName)
			pkValue = key.(types.GenericInnerLink).Pk
			if pkValue == "" {
				return nil, nil
			}
		}

	}

	if node.OnlyLink {
		fields = []*meta.FieldDescription{objectMeta.Key}
	}

	obj, err := sc.dm.Get(objectMeta, fields, objectMeta.Key.Name, pkValue, sc.DbTransaction)
	if err != nil || obj == nil {
		return nil, err
	}
	//return full record
	if node.IsOfGenericType() {
		obj[types.GenericInnerLinkObjectKey] = objectMeta.Name
	}
	return obj, nil
}

func (node *Node) ResolveRegularPlural(sc SearchContext, key interface{}) ([]interface{}, error) {
	logger.Debug("Resolving plural: node [meta=%s, depth=%s, plural=%s], sc=%s, key=%s", node.Meta.Name, node.Depth, node.plural, sc, key)
	var fields []*meta.FieldDescription = nil
	if node.OnlyLink {
		fields = []*meta.FieldDescription{node.Meta.Key}
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
				result[i] = obj
			}
		}
		return result, nil
	}
}

//Resolve records referenced by generic outer field
func (node *Node) ResolveGenericPlural(sc SearchContext, key interface{}, objectMeta *meta.Meta) ([]interface{}, error) {
	logger.Debug("Resolving generic plural: node [meta=%s, depth=%s, plural=%s], sc=%s, key=%s", node.Meta.Name, node.Depth, node.plural, sc, key)
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
				result[i] = obj
			}
		}
		return result, nil
	}
}

func (node *Node) ResolvePluralObjects(sc SearchContext, key interface{}) ([]interface{}, error) {
	var fields []*meta.FieldDescription = nil
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
				result[i] = obj
			}
		}
		return result, nil
	}
}

func (node *Node) fillDirectChildNodes(depthLimit int, fieldMode description.FieldMode) {
	//process regular links, skip generic child nodes
	if node.Meta != nil {
		for i, fieldDescription := range node.Meta.Fields {
			//skip outer link which does not have retrieve mode set to true
			if fieldDescription.LinkType == description.LinkTypeOuter && !fieldDescription.RetrieveMode && fieldMode != description.FieldModeQuery {
				continue
			}
			var onlyLink = false
			branches := make(map[string]*Node)
			if node.Depth == depthLimit {
				onlyLink = true
			}

			if fieldDescription.Type == description.FieldTypeObject && fieldDescription.LinkType == description.LinkTypeInner && (node.Parent == nil || !isBackLink(node.Parent.Meta, &fieldDescription)) {
				node.ChildNodes[fieldDescription.Name] = &Node{
					LinkField:  &node.Meta.Fields[i],
					KeyField:   fieldDescription.LinkMeta.Key,
					Meta:       fieldDescription.LinkMeta,
					ChildNodes: branches,
					Depth:      node.Depth + 1,
					OnlyLink:   onlyLink,
					plural:     false,
					Parent:     node,
					Type:       NodeTypeRegular,
				}
			} else if fieldDescription.Type == description.FieldTypeArray && fieldDescription.LinkType == description.LinkTypeOuter {
				node.ChildNodes[fieldDescription.Name] = &Node{
					LinkField:  &node.Meta.Fields[i],
					KeyField:   fieldDescription.OuterLinkField,
					Meta:       fieldDescription.LinkMeta,
					ChildNodes: branches,
					Depth:      node.Depth + 1,
					OnlyLink:   onlyLink,
					plural:     true,
					Parent:     node,
					Type:       NodeTypeRegular,
				}
			} else if fieldDescription.Type == description.FieldTypeObjects {
				node.ChildNodes[fieldDescription.Name] = &Node{
					LinkField:  &node.Meta.Fields[i],
					KeyField:   fieldDescription.LinkThrough.FindField(fieldDescription.LinkMeta.Name),
					Meta:       fieldDescription.LinkThrough,
					ChildNodes: branches,
					Depth:      node.Depth + 1,
					OnlyLink:   onlyLink,
					plural:     true,
					Parent:     node,
					Type:       NodeTypeRegular,
				}
			} else if fieldDescription.Type == description.FieldTypeGeneric {
				if fieldDescription.LinkType == description.LinkTypeInner {
					node.ChildNodes[fieldDescription.Name] = &Node{
						LinkField:  &node.Meta.Fields[i],
						KeyField:   nil,
						Meta:       nil,
						ChildNodes: branches,
						Depth:      node.Depth + 1,
						OnlyLink:   onlyLink,
						plural:     false,
						Parent:     node,
						MetaList:   fieldDescription.LinkMetaList,
						Type:       NodeTypeGeneric,
					}
				} else if fieldDescription.LinkType == description.LinkTypeOuter {
					node.ChildNodes[fieldDescription.Name] = &Node{
						LinkField:  &node.Meta.Fields[i],
						KeyField:   fieldDescription.OuterLinkField,
						Meta:       fieldDescription.LinkMeta,
						ChildNodes: branches,
						Depth:      node.Depth + 1,
						OnlyLink:   onlyLink,
						plural:     true,
						Parent:     node,
						Type:       NodeTypeGeneric,
					}
				}
			}
		}
	}
}

func (node *Node) RecursivelyFillChildNodes(depthLimit int, fieldMode description.FieldMode) {
	node.fillDirectChildNodes(depthLimit, fieldMode)
	if node.IsOfGenericType() {
		return
	}
	nodesToProcess := make([]*Node, 0)
	for _, v := range node.ChildNodes {
		nodesToProcess = append(nodesToProcess, v)
	}
	processedNodesNames := map[string]bool{node.Meta.Name: true}
	//recursively fill all dependent nodes beginning from root node
	for ; len(nodesToProcess) > 0; nodesToProcess = nodesToProcess[1:] {
		if !nodesToProcess[0].OnlyLink {
			if nodesToProcess[0].IsOfRegularType() || (nodesToProcess[0].IsOfGenericType() && nodesToProcess[0].plural) {
				nodesToProcess[0].fillDirectChildNodes(depthLimit, fieldMode)
				processedNodesNames[nodesToProcess[0].Meta.Name] = true
				for _, childNode := range nodesToProcess[0].ChildNodes {
					//generic fields` meta could not be resolved without fields value
					if childNode.IsOfRegularType() && !processedNodesNames[childNode.Meta.Name] || childNode.IsOfGenericType() {
						nodesToProcess = append(nodesToProcess, childNode)
					}
				}
			}
		}
	}

}

func (node *Node) FillRecordValues(record map[string]interface{}, searchContext SearchContext) map[string]interface{} {
	nodeCopy := node
	//node may mutate during resolving of generic fields, thus local copy of node is required
	for nodeResults := []ResultNode{{nodeCopy, record}}; len(nodeResults) > 0; nodeResults = nodeResults[1:] {
		childNodesResults, _ := nodeResults[0].getFilledChildNodes(searchContext)
		nodeResults = append(nodeResults, childNodesResults...)
	}
	return transformValues(record)
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
		case types.GenericInnerLink:
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
