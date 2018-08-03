package data

import (
	"logger"
	"server/meta"
	"github.com/Q-CIS-DEV/go-rql-parser"
	"server/data/types"
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

func (node *Node) ResolveByRql(sc SearchContext, rqlNode *rqlParser.RqlRootNode) ([]map[string]interface{}, error) {
	return sc.dm.GetRql(node, rqlNode, nil, sc.Tx)
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

	obj, err := sc.dm.Get(objectMeta, fields, objectMeta.Key.Name, pkValue, sc.Tx)
	if err != nil || obj == nil {
		return nil, err
	}
	//return full record
	if !node.OnlyLink {
		if node.IsOfGenericType() {
			obj[types.GenericInnerLinkObjectKey] = objectMeta.Name
		}
		return obj, nil
	} else {
		//return pk only
		if keyStr, err := node.keyAsString(obj, objectMeta); err != nil {
			return nil, err
		} else {
			if node.IsOfGenericType() {
				return map[string]string{types.GenericInnerLinkObjectKey: objectMeta.Name, objectMeta.Key.Name: keyStr}, nil
			} else {
				return keyStr, nil
			}
		}
	}
}

func (node *Node) ResolveRegularPlural(sc SearchContext, key interface{}) ([]interface{}, error) {
	logger.Debug("Resolving plural: node [meta=%s, depth=%s, plural=%s], sc=%s, key=%s", node.Meta.Name, node.Depth, node.plural, sc, key)
	var fields []*meta.FieldDescription = nil
	if node.OnlyLink {
		fields = []*meta.FieldDescription{node.Meta.Key}
	}
	if records, err := sc.dm.GetAll(node.Meta, fields, map[string]interface{}{node.KeyField.Name: key}, sc.Tx); err != nil {
		return nil, err
	} else {
		result := make([]interface{}, len(records), len(records))
		if node.OnlyLink {
			for i, record := range records {
				if keyStr, err := node.keyAsString(record, node.Meta); err != nil {
					return nil, err
				} else {
					result[i] = keyStr
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
	}, sc.Tx); err != nil {
		return nil, err
	} else {
		result := make([]interface{}, len(records), len(records))
		if node.OnlyLink {
			for i, obj := range records {
				keyStr, err := node.keyAsString(obj, node.Meta)
				if err != nil {
					return nil, err
				}
				result[i] = keyStr
			}
		} else {
			for i, obj := range records {
				result[i] = obj
			}
		}
		return result, nil
	}
}

func (node *Node) fillDirectChildNodes(depthLimit int) {
	//process regular links, skip generic child nodes
	if node.Meta != nil {
		for i, fieldDescription := range node.Meta.Fields {
			var onlyLink = false
			var branches map[string]*Node = nil
			if node.Depth == depthLimit {
				onlyLink = true
			} else {
				branches = make(map[string]*Node)
			}

			if fieldDescription.Type == meta.FieldTypeObject && fieldDescription.LinkType == meta.LinkTypeInner && (node.Parent == nil || !isBackLink(node.Parent.Meta, &fieldDescription)) {
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
			} else if fieldDescription.Type == meta.FieldTypeArray && fieldDescription.LinkType == meta.LinkTypeOuter {

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
			} else if fieldDescription.Type == meta.FieldTypeGeneric {
				if fieldDescription.LinkType == meta.LinkTypeInner {
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
				} else if fieldDescription.LinkType == meta.LinkTypeOuter {
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

func (node *Node) RecursivelyFillChildNodes(depthLimit int) {
	node.fillDirectChildNodes(depthLimit)

	nodesToProcess := make([]*Node, 0)
	for _, v := range node.ChildNodes {
		nodesToProcess = append(nodesToProcess, v)
	}
	processedNodesNames := map[string]bool{node.Meta.Name: true}
	//recursively fill all dependent nodes beginning from root node
	for ; len(nodesToProcess) > 0; nodesToProcess = nodesToProcess[1:] {
		if !nodesToProcess[0].OnlyLink {
			if nodesToProcess[0].IsOfRegularType() || (nodesToProcess[0].IsOfGenericType() && nodesToProcess[0].plural) {
				nodesToProcess[0].fillDirectChildNodes(depthLimit)
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

func (node *Node) FillRecordValues(record *map[string]interface{}, searchContext SearchContext) {
	nodeCopy:= node
	//node may mutate during resolving of generic fields, thus local copy of node is required
	for nodeResults := []ResultNode{{nodeCopy, *record}}; len(nodeResults) > 0; nodeResults = nodeResults[1:] {
		childNodesResults, _ := nodeResults[0].getFilledChildNodes(searchContext)
		nodeResults = append(nodeResults, childNodesResults...)
	}
}

func (node *Node) IsOfGenericType() bool {
	return node.Type == NodeTypeGeneric
}

func (node *Node) IsOfRegularType() bool {
	return node.Type == NodeTypeRegular
}
