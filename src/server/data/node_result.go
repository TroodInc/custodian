package data

import "server/meta"

type NodeResult struct {
	node   *Node
	values map[string]interface{}
}

//Replace link values with its objects` full extended value
func (nodeResult NodeResult) getFilledChildNodes(ctx SearchContext) ([]NodeResult, error) {
	childNodeResults := make([]NodeResult, 0)
	for _, childNode := range nodeResult.node.ChildNodes {
		if childNode.LinkField.LinkType == meta.LinkTypeOuter && childNode.LinkField.Type == meta.FieldTypeArray {
			k := nodeResult.values[childNode.Meta.Key.Name]
			if arr, e := childNode.ResolvePlural(ctx, k); e != nil {
				return nil, e
			} else if arr != nil {
				nodeResult.values[childNode.LinkField.Name] = arr
				for _, m := range arr {
					if !childNode.OnlyLink {
						childNodeResults = append(childNodeResults, NodeResult{childNode, m.(map[string]interface{})})
					}
				}
			} else {
				delete(nodeResult.values, childNode.LinkField.Name)
			}
		} else if childNode.LinkField.LinkType == meta.LinkTypeOuter {
			k := nodeResult.values[childNode.Meta.Key.Name]
			if i, e := childNode.Resolve(ctx, k); e != nil {
				return nil, e
			} else if i != nil {
				nodeResult.values[childNode.LinkField.Name] = i
				if !childNode.OnlyLink {
					childNodeResults = append(childNodeResults, NodeResult{childNode, i.(map[string]interface{})})
				}
			} else {
				delete(nodeResult.values, childNode.LinkField.Name)
			}
		} else if childNode.LinkField.LinkType == meta.LinkTypeInner {
			k := nodeResult.values[childNode.LinkField.Name]
			if i, e := childNode.Resolve(ctx, k); e != nil {
				return nil, e
			} else if i != nil {
				nodeResult.values[childNode.LinkField.Name] = i
				if !childNode.OnlyLink {
					childNodeResults = append(childNodeResults, NodeResult{childNode, i.(map[string]interface{})})
				}
			} else {
				delete(nodeResult.values, childNode.LinkField.Name)
			}
		}
	}
	return childNodeResults, nil
}
