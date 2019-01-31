package data

import (
	"strings"
	"fmt"
	"server/object/description"
)

/*
TODO: Query_* files should be moved to separate package, but now it`s not possible because of cyclic imports
*/

type AbstractRetrievePolicy interface {
	Apply(*Node) (*Node, error)
}

type IncludeNodeRetrievePolicy struct {
	pathItems []string
}

func (ip *IncludeNodeRetrievePolicy) Apply(node *Node) (*Node, error) {
	currentNode := node
	var onlyLink bool
	for i := range ip.pathItems {
		currentNodeName := ip.pathItems[i]

		fieldDescription := currentNode.Meta.FindField(currentNodeName)
		if fieldDescription == nil {
			return nil, NewQueryError(
				ErrCodeFieldNotFound,
				fmt.Sprintf(
					"Field '%s' referenced in include path '%s' was not found",
					currentNodeName, fmt.Sprintln(ip.pathItems),
				),

			)
		}
		onlyLink = i == len(ip.pathItems)
		currentNode = currentNode.FillChildNode(fieldDescription, onlyLink, description.FieldModeRetrieve)
	}
	return node, nil
}

func NewIncludeNodeRetrievePolicy(path string) *IncludeNodeRetrievePolicy {
	return &IncludeNodeRetrievePolicy{pathItems: strings.Split(path, ".")}
}

type ExcludeNodeRetrievePolicy struct {
	pathItems []string
}

func (ep *ExcludeNodeRetrievePolicy) Apply(node *Node) (*Node, error) {
	currentNode := node
	var onlyLink bool
	for i := range ep.pathItems {
		currentNodeName := ep.pathItems[i]
		if _, ok := currentNode.ChildNodes[currentNodeName]; !ok {
			return nil, NewQueryError(
				ErrCodeFieldNotFound,
				fmt.Sprintf(
					"Field '%s' referenced in exclude path '%s' was not found",
					currentNodeName, fmt.Sprintln(ep.pathItems),
				),
			)
		} else {
			if !onlyLink {
				delete(currentNode.ChildNodes, currentNodeName)
			}
		}
	}
	return node, nil
}

func NewExcludeNodeRetrievePolicy(path string) *ExcludeNodeRetrievePolicy {
	return &ExcludeNodeRetrievePolicy{pathItems: strings.Split(path, ".")}
}

type RetrievePolicy struct {
	childPolicies []AbstractRetrievePolicy
}

func (rp *RetrievePolicy) Apply(node *Node) (*Node, error) {
	var err error
	for _, retrievePolicy := range rp.childPolicies {
		node, err = retrievePolicy.Apply(node)
		if err != nil {
			return nil, err
		}
	}
	return node, nil
}

type RetrievePolicyFactory struct {
}

func (*RetrievePolicyFactory) Factory(includePaths, excludePaths []string) *RetrievePolicy {
	childPolicies := make([]AbstractRetrievePolicy, 0)
	for _, includePath := range includePaths {
		childPolicies = append(childPolicies, NewIncludeNodeRetrievePolicy(includePath))
	}
	for _, excludePath := range excludePaths {
		childPolicies = append(childPolicies, NewExcludeNodeRetrievePolicy(excludePath))
	}
	return &RetrievePolicy{childPolicies: childPolicies}
}
