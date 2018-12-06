package meta

import (
	. "server/object/description"
	"fmt"
)

type MetaFactory struct {
	builtMetas     map[string]*Meta
	metaDriver     MetaDriver
	metasToResolve []*Meta
}

// factory Meta by provided MetaDescription
func (metaFactory *MetaFactory) FactoryMeta(objectMetaDescription *MetaDescription) (*Meta, error) {
	metaFactory.reset()

	// pre-validate description
	if ok, err := (&ValidationService{}).Validate(objectMetaDescription); !ok {
		return nil, err
	}

	//create object meta
	objectMeta := &Meta{MetaDescription: objectMetaDescription}
	//root object is built manually, thus it should be placed to builtMetas manually too
	metaFactory.builtMetas[objectMeta.Name] = objectMeta
	// enqueue it for resolving
	metaFactory.enqueueForResolving(objectMeta)

	//start resolving enqueued Metas
	for {
		if currentMeta := metaFactory.popMetaToResolve(); currentMeta != nil {
			if err := metaFactory.resolveMeta(currentMeta); err != nil {
				return nil, err
			}
		} else {
			break
		}
	}
	if err := metaFactory.checkOuterLinks(objectMeta); err != nil {
		return nil, err
	}
	if err := metaFactory.setOuterLinks(objectMeta); err != nil {
		return nil, err
	}
	return objectMeta, nil

}

// fill Meta with all required attributes
func (metaFactory *MetaFactory) resolveMeta(currentMeta *Meta) (error) {
	//factory fields
	currentMeta.Fields = make([]FieldDescription, 0, len(currentMeta.MetaDescription.Fields))
	for _, field := range currentMeta.MetaDescription.Fields {
		//field description factory may require to build another Meta, this Meta will be enqueued and processed
		if fieldDescription, err := metaFactory.factoryFieldDescription(field, currentMeta); err != nil {
			return err
		} else {
			currentMeta.Fields = append(currentMeta.Fields, *fieldDescription)
		}
	}

	//factory actionSet
	if actionSet, err := newActionSet(currentMeta.MetaDescription.Actions); err == nil {
		currentMeta.ActionSet = actionSet
	} else {
		return err
	}

	//check PK field
	if currentMeta.Key = currentMeta.FindField(currentMeta.MetaDescription.Key); currentMeta.Key == nil {
		return NewMetaError(currentMeta.Name, "new_meta", ErrNotValid, "Meta '%s' is incorrect. The specified key '%s' Field not found", currentMeta.MetaDescription.Name, currentMeta.MetaDescription.Key)
	} else if !currentMeta.Key.IsSimple() {
		return NewMetaError(currentMeta.Name, "new_meta", ErrNotValid, "Meta '%s' is incorrect. The key Field '%s' is not simple", currentMeta.MetaDescription.Name, currentMeta.MetaDescription.Key)
	}

	// check CAS
	if currentMeta.Cas {
		if cas := currentMeta.FindField("cas"); cas != nil {
			if cas.Type != FieldTypeNumber {
				return NewMetaError(currentMeta.Name, "new_meta", ErrNotValid, "The filed 'cas' specified in the MetaDescription '%s' as CAS must be type of 'number'", currentMeta.MetaDescription.Cas, currentMeta.MetaDescription.Name)
			}
		} else {
			return NewMetaError(currentMeta.Name, "new_meta", ErrNotValid, "Meta '%s' has CAS defined but the filed 'cas' it refers to is absent", currentMeta.MetaDescription.Name, currentMeta.MetaDescription.Cas)
		}
	}
	return nil

}

// get Meta by name and determine if it should be built
func (metaFactory *MetaFactory) buildMeta(metaName string) (metaObj *Meta, shouldBuild bool, err error) {
	if metaObj, ok := metaFactory.builtMetas[metaName]; ok {
		return metaObj, false, nil
	}

	metaDescription, _, err := metaFactory.metaDriver.Get(metaName)
	if err != nil {
		return nil, false, err
	}
	metaObj = &Meta{MetaDescription: metaDescription}
	metaFactory.builtMetas[metaName] = metaObj
	return metaObj, true, nil

}

func (metaFactory *MetaFactory) buildThroughMeta(field *Field, ownerMeta *Meta, ) (metaObj *Meta, shouldBuild bool) {
	metaName := fmt.Sprintf("%s__%s", ownerMeta.Name, field.LinkMeta)

	if metaObj, ok := metaFactory.builtMetas[metaName]; ok {
		return metaObj, false
	}

	fields := []Field{
		{
			Name: "id",
			Type: FieldTypeNumber,
			Def: map[string]interface{}{
				"func": "nextval",
			},
			Optional: true,
		},
		{
			Name:     ownerMeta.Name,
			Type:     FieldTypeObject,
			LinkMeta: ownerMeta.Name,
			LinkType: LinkTypeInner,
			Optional: false,
		},
		{
			Name:     field.LinkMeta,
			Type:     FieldTypeObject,
			LinkMeta: field.LinkMeta,
			LinkType: LinkTypeInner,
			Optional: false,
		},
	}
	//set outer link to the current field
	field.OuterLinkField = fields[1].OuterLinkField
	//
	metaDescription := NewMetaDescription(metaName, "id", fields, []Action{}, false)
	metaObj = &Meta{MetaDescription: metaDescription}
	return metaObj, true

}

//factory field description by provided Field
func (metaFactory *MetaFactory) factoryFieldDescription(field Field, objectMeta *Meta) (*FieldDescription, error) {
	var err error
	var onDeleteStrategy OnDeleteStrategy
	if field.Type == FieldTypeObject || (field.Type == FieldTypeGeneric && field.LinkType == LinkTypeInner) {
		onDeleteStrategy, err = GetOnDeleteStrategyByVerboseName(field.OnDelete)
		if err != nil {
			return nil, NewMetaError(objectMeta.Name, "new_meta", ErrNotValid, "Failed to validate %s's onDelete strategy. %s", field.Name, err.Error())
		}
	}

	fieldDescription := FieldDescription{
		Field:          &field,
		Meta:           objectMeta,
		LinkMeta:       nil,
		OuterLinkField: nil,
		LinkMetaList:   &MetaList{},
		OnDelete:       onDeleteStrategy,
	}

	if field.LinkMeta != "" {

		var shouldBuild bool
		if fieldDescription.LinkMeta, shouldBuild, err = metaFactory.buildMeta(field.LinkMeta); err != nil {
			return nil, err
		}
		if shouldBuild {
			metaFactory.enqueueForResolving(fieldDescription.LinkMeta)
		}
	}

	if field.Type == FieldTypeObjects {

		var shouldBuild bool
		if fieldDescription.LinkThrough, shouldBuild = metaFactory.buildThroughMeta(&field, objectMeta); err != nil {
			return nil, err
		}
		if shouldBuild {
			metaFactory.enqueueForResolving(fieldDescription.LinkThrough)
		}
	}

	if len(field.LinkMetaList) > 0 {
		for _, metaName := range field.LinkMetaList {
			if linkMeta, shouldBuild, err := metaFactory.buildMeta(metaName); err != nil {
				return nil, NewMetaError(objectMeta.Name, "new_meta", ErrNotFound, "Generic field references meta %s, which does not exist", metaName)
			} else {
				fieldDescription.LinkMetaList.AddMeta(linkMeta)
				if shouldBuild {
					metaFactory.enqueueForResolving(linkMeta)
				}
			}
		}
	}
	return &fieldDescription, nil
}

//enqueue Meta to build
func (metaFactory *MetaFactory) enqueueForResolving(objectMeta *Meta) {
	metaFactory.metasToResolve = append(metaFactory.metasToResolve, objectMeta)
}

//reset operational containers
func (metaFactory *MetaFactory) reset() {
	metaFactory.builtMetas = make(map[string]*Meta, 0)
	metaFactory.metasToResolve = make([]*Meta, 0)
}

//get actual Meta to build
func (metaFactory *MetaFactory) popMetaToResolve() *Meta {
	var metaToBuild *Meta
	if len(metaFactory.metasToResolve) > 0 {
		metaToBuild, metaFactory.metasToResolve = metaFactory.metasToResolve[0], metaFactory.metasToResolve[1:]
	} else {
		return nil
	}
	return metaToBuild
}

//check outer links for each processed Metal
func (metaFactory *MetaFactory) setOuterLinks(objectMeta *Meta) error {
	for _, currentObjectMeta := range metaFactory.builtMetas {
		//processing outer links
		for i, _ := range currentObjectMeta.Fields {
			field := &currentObjectMeta.Fields[i]
			if field.LinkType != LinkTypeOuter {
				continue
			}
			field.OuterLinkField = field.LinkMeta.FindField(field.Field.OuterLinkField)
		}
	}
	return nil
}

func (metaFactory *MetaFactory) checkOuterLinks(objectMeta *Meta) error {
	for i, _ := range objectMeta.Fields {
		field := &objectMeta.Fields[i]
		if field.LinkType != LinkTypeOuter {
			continue
		}
		if outerLinkField := field.LinkMeta.FindField(field.Field.OuterLinkField); outerLinkField == nil {
			return NewMetaError(objectMeta.Name, "new_meta", ErrNotValid, "Field '%s' has incorrect outer link. Meta '%s' has no Field '%s'", field.Name, field.LinkMeta.Name, field.Field.OuterLinkField)
		} else if !outerLinkField.canBeLinkTo(field.Meta) {
			return NewMetaError(objectMeta.Name, "new_meta", ErrNotValid, "Field '%s' has incorrect outer link. FieldDescription '%s' of MetaDescription '%s' can't refer to MetaDescription '%s'", field.Name, outerLinkField.Name, outerLinkField.Meta.Name, field.Meta.Name)
		}
	}
	return nil
}

func NewMetaFactory(metaDriver MetaDriver) *MetaFactory {
	return &MetaFactory{make(map[string]*Meta, 0), metaDriver, make([]*Meta, 0)}
}
