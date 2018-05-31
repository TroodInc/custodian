package data

import (
	"logger"
	"server/meta"
	"github.com/WhackoJacko/go-rql-parser"
	"fmt"
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
}

func (node *Node) keyAsString(obj map[string]interface{}) (string, error) {
	v := obj[node.Meta.Key.Name]
	str, err := node.Meta.Key.ValueAsString(v)
	return str, err
}

func (node *Node) ResolveByRql(sc SearchContext, rqlNode *rqlParser.RqlRootNode) ([]map[string]interface{}, error) {
	return sc.dm.GetRql(node, rqlNode, nil)
}

func (node *Node) Resolve(sc SearchContext, key interface{}) (interface{}, error) {
	var fields []*meta.FieldDescription = nil
	if node.OnlyLink {
		fields = []*meta.FieldDescription{node.Meta.Key}
	}

	obj, err := sc.dm.Get(node.Meta, fields, node.KeyField.Name, key)
	if err != nil {
		return nil, err
	}

	if obj == nil {
		return nil, nil
	}

	if !node.OnlyLink {
		return obj, nil
	}

	keyStr, err := node.keyAsString(obj)
	if err != nil {
		return nil, err
	}

	return keyStr, nil
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
			keyStr, err := node.keyAsString(obj)
			if err != nil {
				return nil, err
			}
			result[i] = fmt.Sprint(sc.lazyPath, "/", node.Meta.Name, "/", keyStr)
		}
	} else {
		for i, obj := range objs {
			result[i] = obj
		}
	}

	return result, nil
}

func (node *Node) fillDirectChildNodes(depthLimit int) {
	for i, f := range node.Meta.Fields {
		var onlyLink = false
		var branches map[string]*Node = nil
		if node.Depth == depthLimit {
			onlyLink = true
		} else {
			branches = make(map[string]*Node)
		}
		var plural = false
		var keyFiled *meta.FieldDescription = nil

		if f.LinkType == meta.LinkTypeInner && (node.Parent == nil || !isBackLink(node.Parent.Meta, &f)) {
			keyFiled = f.LinkMeta.Key

			node.ChildNodes[f.Name] = &Node{LinkField: &node.Meta.Fields[i],
				KeyField: keyFiled,
				Meta: f.LinkMeta,
				ChildNodes: branches,
				Depth: node.Depth + 1,
				OnlyLink: onlyLink,
				plural: plural,
				Parent: node}
		} else if f.LinkType == meta.LinkTypeOuter {
			keyFiled = f.OuterLinkField
			if f.Type == meta.FieldTypeArray {
				plural = true
			}

			node.ChildNodes[f.Name] = &Node{LinkField: &node.Meta.Fields[i],
				KeyField: keyFiled,
				Meta: f.LinkMeta,
				ChildNodes: branches,
				Depth: node.Depth + 1,
				OnlyLink: onlyLink,
				plural: plural,
				Parent: node}
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
	for nodeResults := []NodeResult{{node, *record}}; len(nodeResults) > 0; nodeResults = nodeResults[1:] {
		childNodesResults, _ := nodeResults[0].getFilledChildNodes(searchContext)
		nodeResults = append(nodeResults, childNodesResults...)
	}
}
