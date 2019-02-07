package data

import (
	"strings"
	"fmt"
	"server/object/description"
)

/*
TODO: Query_* files should be moved to separate package, but now it`s not possible because of cyclic imports
*/

type RetrievePolicy interface {
	Apply(*Node, *AggregatedRetrievePolicy) error
	PathItems() []string
	SubPolicy() RetrievePolicy
}

type includeNodeRetrievePolicy struct {
	pathItems []string
}

func (ip *includeNodeRetrievePolicy) Apply(node *Node, parentPolicy *AggregatedRetrievePolicy) error {
	currentNode := node
	for i := range ip.pathItems {
		currentPath := ip.pathItems[i]
		isLeaf := len(ip.pathItems)-1 == i

		fieldDescription := currentNode.Meta.FindField(currentPath)
		if fieldDescription == nil {
			return NewQueryError(
				ErrCodeFieldNotFound,
				fmt.Sprintf(
					"Field '%s' referenced in 'only' path '%s' was not found",
					currentPath, fmt.Sprintln(ip.pathItems),
				),
			)
		}
		if fieldDescription.Type == description.FieldTypeGeneric && fieldDescription.LinkType == description.LinkTypeInner {
			return nil
		}
		if fieldDescription.IsLink() && (!isLeaf && fieldDescription.LinkType == description.LinkTypeInner || fieldDescription.LinkType == description.LinkTypeOuter) {
			if isLeaf {
				currentNode.ChildNodes.Empty()
			}
			currentNode.OnlyLink = false
			currentNode = currentNode.FillChildNode(fieldDescription, isLeaf, description.FieldModeRetrieve, parentPolicy.SubPolicyForNode(fieldDescription.Name))
		} else if fieldDescription.LinkType != description.LinkTypeOuter {
			//exclude all child nodes, filled by default
			currentNode.ChildNodes.Empty()
			currentNode.SelectFields.Include(fieldDescription)
			currentNode.OnlyLink = false
		}
	}
	return nil
}
func (ip *includeNodeRetrievePolicy) PathItems() []string {
	return ip.pathItems
}

func (ip *includeNodeRetrievePolicy) SubPolicy() RetrievePolicy {
	if len(ip.pathItems) == 1 {
		return nil
	}
	return &includeNodeRetrievePolicy{pathItems: append([]string(nil), ip.pathItems[1:]...)}
}

func newIncludeNodeRetrievePolicy(path string) *includeNodeRetrievePolicy {
	return &includeNodeRetrievePolicy{pathItems: strings.Split(path, ".")}
}

type excludeNodeRetrievePolicy struct {
	pathItems []string
}

func (ep *excludeNodeRetrievePolicy) Apply(node *Node, _ *AggregatedRetrievePolicy) error {
	currentNode := node
	for i := range ep.pathItems {
		currentPath := ep.pathItems[i]
		isLeaf := i == len(ep.pathItems)-1

		fieldDescription := currentNode.Meta.FindField(currentPath)

		if fieldDescription == nil {
			return NewQueryError(
				ErrCodeFieldNotFound,
				fmt.Sprintf(
					"Field '%s' referenced in exclude path '%s' was not found",
					currentPath, fmt.Sprintln(ep.pathItems),
				),
			)
		} else {
			if fieldDescription.Type == description.FieldTypeGeneric && fieldDescription.LinkType == description.LinkTypeInner && !isLeaf {
				return nil
			}
			if isLeaf {
				if fieldDescription.IsLink() {
					currentNode.ChildNodes.Exclude(fieldDescription.Name)
					currentNode.SelectFields.Exclude(fieldDescription)
				} else {
					currentNode.SelectFields.Exclude(fieldDescription)
				}
			} else {
				ok := false
				currentNode, ok = currentNode.ChildNodes.Get(fieldDescription.Name)
				if !ok {
					return NewQueryError(
						ErrCodeFieldNotFound,
						fmt.Sprintf(
							"Branch '%s' referenced in exclude path '%s' was not found",
							currentPath, fmt.Sprintln(ep.pathItems),
						),
					)
				}
			}
		}
	}
	return nil
}

func (ep *excludeNodeRetrievePolicy) PathItems() []string {
	return ep.pathItems
}

func (ep *excludeNodeRetrievePolicy) SubPolicy() RetrievePolicy {
	if len(ep.pathItems) == 1 {
		return nil
	}
	return &excludeNodeRetrievePolicy{pathItems: append([]string(nil), ep.pathItems[1:]...)}
}

func newExcludeNodeRetrievePolicy(path string) *excludeNodeRetrievePolicy {
	return &excludeNodeRetrievePolicy{pathItems: strings.Split(path, ".")}
}

//The only public structure, all policy appliances are done via this one
type AggregatedRetrievePolicy struct {
	childPolicies []RetrievePolicy
}

func (rp *AggregatedRetrievePolicy) Apply(node *Node) error {
	if rp == nil {
		return nil
	}
	var err error
	for _, retrievePolicy := range rp.childPolicies {
		err = retrievePolicy.Apply(node, rp)
		if err != nil {
			return err
		}
	}
	return nil
}

/*Generate a new policy, which is applicable to the given child node
Eg. having parent policy with 3 rules:
	- include account.company.name
	- include account.phone
	- exclude address.description
subPolicy for node "account" will be as follows:
	- include company.name
	- include phone
*/
func (rp *AggregatedRetrievePolicy) SubPolicyForNode(nodeName string) *AggregatedRetrievePolicy {
	if rp == nil {
		return nil
	}
	childPolicies := make([]RetrievePolicy, 0)
	for _, childPolicy := range rp.childPolicies {
		if childPolicy.PathItems()[0] == nodeName {
			if childPolicy = childPolicy.SubPolicy(); childPolicy != nil {
				childPolicies = append(childPolicies, childPolicy)
			}
		}
	}
	if len(childPolicies) > 0 {
		return &AggregatedRetrievePolicy{childPolicies: childPolicies}
	} else {
		return nil
	}
}

type AggregatedRetrievePolicyFactory struct {
}

func (*AggregatedRetrievePolicyFactory) Factory(includePaths, excludePaths []string) *AggregatedRetrievePolicy {
	childPolicies := make([]RetrievePolicy, 0)
	for _, includePath := range includePaths {
		childPolicies = append(childPolicies, newIncludeNodeRetrievePolicy(includePath))
	}
	for _, excludePath := range excludePaths {
		childPolicies = append(childPolicies, newExcludeNodeRetrievePolicy(excludePath))
	}
	return &AggregatedRetrievePolicy{childPolicies: childPolicies}
}
