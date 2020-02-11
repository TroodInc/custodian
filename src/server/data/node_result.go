package data

import (
	"server/data/record"
	"server/data/types"
	"server/object"
)

type ResultNode struct {
	node   *Node
	values *record.Record
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
				keyValue := resultNode.values.Data[childNode.Meta.Key]
				if childNode.LinkField.Type != object.FieldTypeObjects {
					if arr, e := childNode.ResolveRegularPlural(ctx, keyValue); e != nil {
						return nil, e
					} else if arr != nil {
						resultNode.values.Data[childNode.LinkField.Name] = arr
						for _, m := range arr {
							if !childNode.OnlyLink {
								childNodeResults = append(childNodeResults, ResultNode{childNode, m.(*record.Record)})
							}
						}
					} else {
						delete(resultNode.values.Data, childNode.LinkField.Name)
					}
				} else {
					if arr, e := childNode.ResolvePluralObjects(ctx, keyValue); e != nil {
						return nil, e
					} else if arr != nil {
						resultNode.values.Data[childNode.LinkField.Name] = arr
						if !childNode.OnlyLink {
							for _, m := range arr {
								childNodeResults = append(childNodeResults, ResultNode{childNode, m.(*record.Record)})
							}
						}
					} else {
						delete(resultNode.values.Data, childNode.LinkField.Name)
					}
				}
			}
		} else if childNode.plural && childNode.IsOfGenericType() {
			if !ctx.omitOuters {
				pkValue := resultNode.values.Data[childNode.Meta.Key]
				if arr, e := childNode.ResolveGenericPlural(ctx, pkValue, resultNode.node.Meta); e != nil {
					return nil, e
				} else if arr != nil {
					resultNode.values.Data[childNode.LinkField.Name] = arr

					//add node for resolving
					if !childNode.OnlyLink && childNode.Depth < ctx.depthLimit {
						for _, m := range arr {
							childNodeResults = append(childNodeResults, ResultNode{childNode, m.(*record.Record)})
						}
					}
				} else {
					delete(resultNode.values.Data, childNode.LinkField.Name)
				}
			}
		} else if childNode.LinkField.LinkType == object.LinkTypeInner && !childNode.IsOfGenericType() {
			k := resultNode.values.Data[childNode.LinkField.Name]
			if i, e := childNode.Resolve(ctx, k); e != nil {
				return nil, e
			} else if i != nil {
				resultNode.values.Data[childNode.LinkField.Name] = i
				if !childNode.OnlyLink {
					childNodeResults = append(childNodeResults, ResultNode{childNode, i})
				}
			}
		} else if !childNode.plural && childNode.IsOfGenericType() {
			k := resultNode.values.Data[childNode.LinkField.Name]
			//skip resolving if generic field value is nil
			if k == nil {
				continue
			}
			if k, ok := k.(*types.GenericInnerLink); ok && k.ObjectName == ""{
				continue
			}
			//retrieve policy for generic fields is specific for each record, so it should be build on the go
			var retrievePolicyForThisMeta *AggregatedRetrievePolicy
			if resultNode.node.RetrievePolicy != nil {
				retrievePolicyForThisField := resultNode.node.RetrievePolicy.SubPolicyForNode(childNode.LinkField.Name)
				if retrievePolicyForThisField != nil {
					retrievePolicyForThisMeta = retrievePolicyForThisField.SubPolicyForNode(k.(*types.GenericInnerLink).ObjectName)
				}
			}
			//OnlyLink should be determined on the go, because it depends on concrete record and its policies
			defaultOnlyLink := childNode.OnlyLink
			childNode.OnlyLink = childNode.Depth > ctx.depthLimit
			childNode.ChildNodes = *NewChildNodes()
			childNode.RetrievePolicy = retrievePolicyForThisMeta

			var childNodeLinkMeta *object.Meta
			switch k.(type) {
			case *types.GenericInnerLink:
				childNodeLinkMeta = childNode.LinkField.GetLinkMetaByName(k.(*types.GenericInnerLink).ObjectName)
			case *record.Record:
				childNodeLinkMeta = childNode.LinkField.GetLinkMetaByName(k.(*record.Record).Meta.Name)
			}

			childNode.SelectFields = *NewSelectFields(childNodeLinkMeta.GetKey(), childNodeLinkMeta.TableFields())
			childNode.Meta = childNodeLinkMeta
			childNode.KeyField = childNodeLinkMeta.GetKey()
			childNode.RecursivelyFillChildNodes(ctx.depthLimit, object.FieldModeRetrieve)

			if resolvedValue, e := childNode.Resolve(ctx, k); e != nil {
				childNode.OnlyLink = defaultOnlyLink
				return nil, e
			} else if resolvedValue != nil {
				resultNode.values.Data[childNode.LinkField.Name] = resolvedValue

				//dynamically fill child nodes, because child node can be determined only with generic field data
				// values

				if !childNode.OnlyLink {
					childNodeResults = append(childNodeResults, ResultNode{childNode, resolvedValue})
				}
				childNode.OnlyLink = defaultOnlyLink
			} else {
				childNode.OnlyLink = defaultOnlyLink
				delete(resultNode.values.Data, childNode.LinkField.Name)
			}
		}
	}
	return childNodeResults, nil
}
