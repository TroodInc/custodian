package data

import "server/meta"

type ResultNode struct {
	node   *Node
	values map[string]interface{}
}

//Replace link values with its objects` full extended value
func (resultNode ResultNode) getFilledChildNodes(ctx SearchContext) ([]ResultNode, error) {
	childNodeResults := make([]ResultNode, 0)
	for _, childNode := range resultNode.node.ChildNodes {
		if childNode.LinkField.LinkType == meta.LinkTypeOuter && childNode.LinkField.Type == meta.FieldTypeArray {
			k := resultNode.values[childNode.Meta.Key.Name]
			if arr, e := childNode.ResolvePlural(ctx, k); e != nil {
				return nil, e
			} else if arr != nil {
				resultNode.values[childNode.LinkField.Name] = arr
				for _, m := range arr {
					if !childNode.OnlyLink {
						childNodeResults = append(childNodeResults, ResultNode{childNode, m.(map[string]interface{})})
					}
				}
			} else {
				delete(resultNode.values, childNode.LinkField.Name)
			}
		} else if childNode.LinkField.LinkType == meta.LinkTypeOuter {
			k := resultNode.values[childNode.Meta.Key.Name]
			if i, e := childNode.Resolve(ctx, k); e != nil {
				return nil, e
			} else if i != nil {
				resultNode.values[childNode.LinkField.Name] = i
				if !childNode.OnlyLink {
					childNodeResults = append(childNodeResults, ResultNode{childNode, i.(map[string]interface{})})
				}
			} else {
				delete(resultNode.values, childNode.LinkField.Name)
			}
		} else if childNode.LinkField.LinkType == meta.LinkTypeInner {
			k := resultNode.values[childNode.LinkField.Name]
			if i, e := childNode.Resolve(ctx, k); e != nil {
				return nil, e
			} else if i != nil {
				resultNode.values[childNode.LinkField.Name] = i
				if !childNode.OnlyLink {
					childNodeResults = append(childNodeResults, ResultNode{childNode, i.(map[string]interface{})})
				}
			} else {
				delete(resultNode.values, childNode.LinkField.Name)
			}
		}
	}
	return childNodeResults, nil
}
