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
	for _, childNode := range resultNode.node.ChildNodes.Nodes() {

		//if the current level equals to depth limit, only outer links(ie plural nodes) should be resolved
		if !childNode.plural && childNode.OnlyLink && childNode.RetrievePolicy == nil {
			continue
		}

		if childNode.plural && childNode.IsOfRegularType() {
			if !ctx.omitOuters {
				keyValue := resultNode.values[childNode.Meta.Key.Name]
				if childNode.LinkField.Type != description.FieldTypeObjects {
					if arr, e := childNode.ResolveRegularPlural(ctx, keyValue); e != nil {
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
				} else {
					if arr, e := childNode.ResolvePluralObjects(ctx, keyValue); e != nil {
						return nil, e
					} else if arr != nil {
						resultNode.values[childNode.LinkField.Name] = arr
						if !childNode.OnlyLink {
							for _, m := range arr {
								childNodeResults = append(childNodeResults, ResultNode{childNode, m.(map[string]interface{})})
							}
						}
					} else {
						delete(resultNode.values, childNode.LinkField.Name)
					}
				}
			}
		} else if childNode.plural && childNode.IsOfGenericType() {
			if !ctx.omitOuters {
				pkValue := resultNode.values[childNode.Meta.Key.Name]
				if arr, e := childNode.ResolveGenericPlural(ctx, pkValue, resultNode.node.Meta); e != nil {
					return nil, e
				} else if arr != nil {
					resultNode.values[childNode.LinkField.Name] = arr

					//add node for resolving
					if !childNode.OnlyLink && childNode.Depth < ctx.depthLimit {
						for _, m := range arr {
							childNodeResults = append(childNodeResults, ResultNode{childNode, m.(map[string]interface{})})
						}
					}
				} else {
					delete(resultNode.values, childNode.LinkField.Name)
				}
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
			//retrieve policy for generic fields is specific for each record, so it should be build on the go
			var retrievePolicyForThisMeta *AggregatedRetrievePolicy
			if resultNode.node.RetrievePolicy != nil {
				retrievePolicyForThisField := resultNode.node.RetrievePolicy.SubPolicyForNode(childNode.LinkField.Name)
				if retrievePolicyForThisField != nil {
					retrievePolicyForThisMeta = retrievePolicyForThisField.SubPolicyForNode(k.(types.GenericInnerLink).ObjectName)
				}
			}
			//OnlyLink should be determined on the go, because it depends on concrete record and its policies
			defaultOnlyLink := childNode.OnlyLink
			childNode.OnlyLink = childNode.Depth > ctx.depthLimit
			childNode.ChildNodes = *NewChildNodes()
			childNode.RetrievePolicy = retrievePolicyForThisMeta
			childNodeLinkMeta := childNode.LinkField.LinkMetaList.GetByName(k.(types.GenericInnerLink).ObjectName)
			childNode.SelectFields = *NewSelectFields(childNodeLinkMeta.Key, childNodeLinkMeta.TableFields())
			childNode.Meta = childNodeLinkMeta
			childNode.KeyField = childNodeLinkMeta.Key
			childNode.RecursivelyFillChildNodes(ctx.depthLimit, description.FieldModeRetrieve)

			if resolvedValue, e := childNode.Resolve(ctx, k); e != nil {
				childNode.OnlyLink = defaultOnlyLink
				return nil, e
			} else if resolvedValue != nil {
				resultNode.values[childNode.LinkField.Name] = resolvedValue

				//dynamically fill child nodes, because child node can be determined only with generic field data
				// values

				if !childNode.OnlyLink {
					childNodeResults = append(childNodeResults, ResultNode{childNode, resolvedValue.(map[string]interface{})})
				}
				childNode.OnlyLink = defaultOnlyLink
			} else {
				childNode.OnlyLink = defaultOnlyLink
				delete(resultNode.values, childNode.LinkField.Name)
			}
		}
	}
	return childNodeResults, nil
}
