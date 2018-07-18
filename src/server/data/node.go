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
	return sc.dm.GetRql(node, rqlNode, nil)
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
		objectMeta = node.MetaList.GetByName(key.(types.GenericInnerLink).ObjectName)
		pkValue = key.(types.GenericInnerLink).Pk
		if pkValue == "" {
			return nil, nil
		}
	}

	if node.OnlyLink {
		fields = []*meta.FieldDescription{objectMeta.Key}
	}

	obj, err := sc.dm.Get(objectMeta, fields, objectMeta.Key.Name, pkValue)
	if err != nil || obj == nil {
		return nil, err
	}
	//return full record
	if !node.OnlyLink {
		if node.IsOfGenericType() {
			obj[types.GENERIC_INNER_LINK_OBJECT_KEY] = objectMeta.Name
		}
		return obj, nil
	} else {
		//return pk only
		if keyStr, err := node.keyAsString(obj, objectMeta); err != nil {
			return nil, err
		} else {
			if node.IsOfGenericType() {
				return map[string]string{types.GENERIC_INNER_LINK_OBJECT_KEY: objectMeta.Name, types.GENERIC_PK_KEY: keyStr}, nil
			} else {
				return keyStr, nil
			}
		}
	}
}

func (node *Node) ResolvePlural(sc SearchContext, key interface{}) ([]interface{}, error) {
	logger.Debug("Resolving plural: node [meta=%s, depth=%s, plural=%s], sc=%s, key=%s", node.Meta.Name, node.Depth, node.plural, sc, key)
	var fields []*meta.FieldDescription = nil
	if node.OnlyLink {
		fields = []*meta.FieldDescription{node.Meta.Key}
	}

	objs, err := sc.dm.GetAll(node.Meta, fields, node.KeyField.Name, key)
	if err != nil {
		return nil, err
	}

	objsLength := len(objs)
	result := make([]interface{}, objsLength, objsLength)
	if node.OnlyLink {
		for i, obj := range objs {
			keyStr, err := node.keyAsString(obj, node.Meta)
			if err != nil {
				return nil, err
			}
			result[i] = keyStr
		}
	} else {
		for i, obj := range objs {
			result[i] = obj
		}
	}

	return result, nil
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
			var plural = false

			if fieldDescription.Type == meta.FieldTypeObject && fieldDescription.LinkType == meta.LinkTypeInner && (node.Parent == nil || !isBackLink(node.Parent.Meta, &fieldDescription)) {
				node.ChildNodes[fieldDescription.Name] = &Node{
					LinkField:  &node.Meta.Fields[i],
					KeyField:   fieldDescription.LinkMeta.Key,
					Meta:       fieldDescription.LinkMeta,
					ChildNodes: branches,
					Depth:      node.Depth + 1,
					OnlyLink:   onlyLink,
					plural:     plural,
					Parent:     node,
					Type:       NodeTypeRegular,
				}
			} else if fieldDescription.Type == meta.FieldTypeArray && fieldDescription.LinkType == meta.LinkTypeOuter {
				plural = true

				node.ChildNodes[fieldDescription.Name] = &Node{
					LinkField:  &node.Meta.Fields[i],
					KeyField:   fieldDescription.OuterLinkField,
					Meta:       fieldDescription.LinkMeta,
					ChildNodes: branches,
					Depth:      node.Depth + 1,
					OnlyLink:   onlyLink,
					plural:     plural,
					Parent:     node,
					Type:       NodeTypeRegular,
				}
			} else if fieldDescription.Type == meta.FieldTypeGeneric && fieldDescription.LinkType == meta.LinkTypeInner {
				node.ChildNodes[fieldDescription.Name] = &Node{
					LinkField:  &node.Meta.Fields[i],
					KeyField:   nil,
					Meta:       nil,
					ChildNodes: branches,
					Depth:      node.Depth + 1,
					OnlyLink:   onlyLink,
					plural:     plural,
					Parent:     node,
					MetaList:   fieldDescription.LinkMetaList,
					Type:       NodeTypeGeneric,
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
		if !nodesToProcess[0].OnlyLink && nodesToProcess[0].IsOfRegularType() {
			nodesToProcess[0].fillDirectChildNodes(depthLimit)
			processedNodesNames[nodesToProcess[0].Meta.Name] = true
			for _, childNode := range nodesToProcess[0].ChildNodes {
				if !processedNodesNames[childNode.Meta.Name] {
					nodesToProcess = append(nodesToProcess, childNode)
				}
			}
		}
	}
}

func (node *Node) FillRecordValues(record *map[string]interface{}, searchContext SearchContext) {
	for nodeResults := []ResultNode{{node, *record}}; len(nodeResults) > 0; nodeResults = nodeResults[1:] {
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
