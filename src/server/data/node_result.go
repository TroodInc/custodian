package data

import (
	"server/data/types"
	"server/object/description"
)

type ResultNode struct {
	node   *Node
	values map[string]interface{}
}

//Replace link values with its objects` full extended value
func (resultNode ResultNode) getFilledChildNodes(ctx SearchContext) ([]ResultNode, error) {
	childNodeResults := make([]ResultNode, 0)
	for _, childNode := range resultNode.node.ChildNodes {
		if childNode.plural && childNode.IsOfRegularType() {
			k := resultNode.values[childNode.Meta.Key.Name]
			if arr, e := childNode.ResolveRegularPlural(ctx, k); e != nil {
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
		} else if childNode.plural && childNode.IsOfGenericType() {
			pkValue := resultNode.values[childNode.Meta.Key.Name]
			if arr, e := childNode.ResolveGenericPlural(ctx, pkValue, resultNode.node.Meta); e != nil {
				return nil, e
			} else if arr != nil {
				resultNode.values[childNode.LinkField.Name] = arr
				for _, m := range arr {
					if childNode.Depth < ctx.depthLimit {
						childNodeResults = append(childNodeResults, ResultNode{childNode, m.(map[string]interface{})})
					}
				}
			} else {
				delete(resultNode.values, childNode.LinkField.Name)
			}
		} else if childNode.LinkField.LinkType == description.LinkTypeInner && !childNode.IsOfGenericType() {
			k := resultNode.values[childNode.LinkField.Name]
			if i, e := childNode.Resolve(ctx, k); e != nil {
				return nil, e
			} else if i != nil {
				resultNode.values[childNode.LinkField.Name] = i
				if !childNode.OnlyLink {
					childNodeResults = append(childNodeResults, ResultNode{childNode, i.(map[string]interface{})})
				}
			}
		} else if !childNode.plural && childNode.IsOfGenericType() {
			k := resultNode.values[childNode.LinkField.Name]
			//skip resolving if generic field value is nil
			if k == nil || k.(types.GenericInnerLink).ObjectName == "" {
				continue
			}
			if resolvedValue, e := childNode.Resolve(ctx, k); e != nil {
				return nil, e
			} else if resolvedValue != nil {
				resultNode.values[childNode.LinkField.Name] = resolvedValue
				if childNode.Depth <= ctx.depthLimit {
					//dynamically fill child nodes, because child node can be determined only with generic field data
					// values

					childNodeLinkMeta := childNode.LinkField.LinkMetaList.GetByName(resolvedValue.(map[string]interface{})[types.GenericInnerLinkObjectKey].(string))
					childNode.Meta = childNodeLinkMeta
					childNode.KeyField = childNodeLinkMeta.Key
					childNode.RecursivelyFillChildNodes(ctx.depthLimit - childNode.Depth)
				}

				if !childNode.OnlyLink {
					childNodeResults = append(childNodeResults, ResultNode{childNode, resolvedValue.(map[string]interface{})})
				}
			} else {
				delete(resultNode.values, childNode.LinkField.Name)
			}
		}
	}
	return childNodeResults, nil
}
