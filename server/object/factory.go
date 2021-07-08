package object

import (
	"custodian/server/errors"
	. "custodian/server/object/description"
	"fmt"
)

type MetaFactory struct {
	builtMetas     map[string]*Meta
	metaDriver     MetaDescriptionSyncer
	metasToResolve []*Meta
}

// factory MetaDescription by provided MetaDescription
func (metaFactory *MetaFactory) FactoryMeta(objectMetaDescription *MetaDescription) (*Meta, error) {
	metaFactory.reset()

	// pre-validate description
	if ok, err := (&MetaValidationService{}).Validate(objectMetaDescription); !ok {
		return nil, err
	}

	//create object meta
	objectMeta := &Meta{MetaDescription: objectMetaDescription}
	//root object is built manually, thus it should be placed to builtMetas manually too
	metaFactory.builtMetas[objectMeta.Name] = objectMeta
	// enqueue it for resolving
	metaFactory.enqueueForResolving(objectMeta)

	if err := metaFactory.resolveEnqueued(); err != nil {
		return nil, err
	}

	if err := metaFactory.checkOuterLinks(objectMeta); err != nil {
		return nil, err
	}
	if err := metaFactory.setOuterLinks(objectMeta); err != nil {
		return nil, err
	}
	if err := metaFactory.setObjectsLinks(objectMeta); err != nil {
		return nil, err
	}
	return objectMeta, nil
}

func (metaFactory *MetaFactory) resolveEnqueued() error {
	//recursively resolve meta
	for {
		if currentMeta := metaFactory.popMetaToResolve(); currentMeta != nil {
			if err := metaFactory.resolveMeta(currentMeta); err != nil {
				return err
			} else {
				//TODO: actions like "checkOuterLinks", "setOuterLinks", "setObjectsLinks" should be performed for each
				//meta regardless of whether meta is root or not, thus operations made in this case section duplicate
				//operations for root meta. This behaviour should be fixed and tested to ensure no circular resolving
				//happens
				if err := metaFactory.setOuterLinks(currentMeta); err != nil {
					return err
				}
			}
		} else {
			break
		}
	}
	return nil
}

// fill MetaDescription with all required attributes
func (metaFactory *MetaFactory) resolveMeta(currentMeta *Meta) error {
	//factory fields
	currentMeta.Fields = make([]FieldDescription, 0, len(currentMeta.MetaDescription.Fields))
	for _, field := range currentMeta.MetaDescription.Fields {
		//field description factory may require to build another MetaDescription, this MetaDescription will be enqueued and processed
		if fieldDescription, err := metaFactory.factoryFieldDescription(field, currentMeta); err != nil {
			return err
		} else {
			currentMeta.Fields = append(currentMeta.Fields, *fieldDescription)
		}
	}

	//factory actions
	for _, action := range currentMeta.MetaDescription.Actions {
		a, err := InitAction(&action)
		if err == nil {
			currentMeta.Actions = append(currentMeta.Actions, a)
		}
	}

	//check PK field
	if currentMeta.Key = currentMeta.FindField(currentMeta.MetaDescription.Key); currentMeta.Key == nil {
		return errors.NewValidationError(
			"new_meta",
			fmt.Sprintf("MetaDescription '%s' is incorrect. The specified key '%s' Field not found", currentMeta.MetaDescription.Name, currentMeta.MetaDescription.Key),
			nil,
		)
	} else if !currentMeta.Key.IsSimple() {
		return errors.NewValidationError(
			"new_meta",
			fmt.Sprintf("MetaDescription '%s' is incorrect. The key Field '%s' is not simple", currentMeta.MetaDescription.Name, currentMeta.MetaDescription.Key),
			nil,
		)
	}

	// check CAS
	if currentMeta.Cas {
		if cas := currentMeta.FindField("cas"); cas != nil {
			if cas.Type != FieldTypeNumber {
				return errors.NewValidationError(
					"new_meta",
					fmt.Sprintf("The filed 'cas' specified in the MetaDescription '%s' as CAS must be type of 'number'", currentMeta.MetaDescription.Name),
					nil,
				)
			}
		} else {
			return errors.NewValidationError(
				"new_meta",
				fmt.Sprintf("MetaDescription '%s' has CAS defined but the filed 'cas' it refers to is absent", currentMeta.MetaDescription.Name),
				nil,
			)
		}
	}
	return nil

}

// get MetaDescription by name and determine if it should be built
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

	metaFactory.metaDriver.Cache().Set(metaObj)

	return metaObj, true, nil

}

func (metaFactory *MetaFactory) FactoryFieldDescription(field Field, objectMeta *Meta) (*FieldDescription, error) {
	//public method to factory field and resolve metas
	fieldDescription, err := metaFactory.factoryFieldDescription(field, objectMeta)
	if err != nil {
		return nil, err
	} else {
		if err := metaFactory.resolveEnqueued(); err != nil {
			return nil, err
		}
		return fieldDescription, nil
	}
}

//factory field description by provided NewField
func (metaFactory *MetaFactory) buildThroughMeta(field *Field, ownerMeta *Meta) (metaObj *Meta, shouldBuild bool) {
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
	field.OuterLinkField = fields[1].Name
	//
	metaDescription := NewMetaDescription(metaName, "id", fields, []Action{}, false)
	metaObj = &Meta{MetaDescription: metaDescription}

	metaFactory.metaDriver.Cache().Set(metaObj)

	return metaObj, true

}

//factory field description by provided Field
func (metaFactory *MetaFactory) factoryFieldDescription(field Field, objectMeta *Meta) (*FieldDescription, error) {
	var err error

	fieldDescription := FieldDescription{
		Field:          &field,
		Meta:           objectMeta,
		LinkMeta:       nil,
		OuterLinkField: nil,
		LinkMetaList:   &MetaList{},
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

	if field.Type == FieldTypeObjects && field.LinkType == LinkTypeInner {

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
				return nil, errors.NewValidationError(
					"new_meta",
					fmt.Sprintf("Generic field references meta %s, which does not exist", metaName),
					nil,
				)
			} else {
				fieldDescription.LinkMetaList.AddMeta(linkMeta)
				metaFactory.metaDriver.Cache().Set(linkMeta)

				if shouldBuild {
					metaFactory.enqueueForResolving(linkMeta)
				}
			}
		}
	}
	return &fieldDescription, nil
}

//enqueue MetaDescription to build
func (metaFactory *MetaFactory) enqueueForResolving(objectMeta *Meta) {
	metaFactory.metasToResolve = append(metaFactory.metasToResolve, objectMeta)
}

//reset operational containers
func (metaFactory *MetaFactory) reset() {
	metaFactory.builtMetas = make(map[string]*Meta, 0)
	metaFactory.metasToResolve = make([]*Meta, 0)
}

//get actual MetaDescription to build
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

//check outer links for each processed Metal
func (metaFactory *MetaFactory) setObjectsLinks(objectMeta *Meta) error {
	for _, currentObjectMeta := range metaFactory.builtMetas {
		//processing outer links
		for i, _ := range currentObjectMeta.Fields {
			field := &currentObjectMeta.Fields[i]
			if field.Type != FieldTypeObjects {
				continue
			}
			field.OuterLinkField = field.LinkThrough.FindField(field.Field.OuterLinkField)
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
			return errors.NewValidationError(
				"new_meta",
				fmt.Sprintf("Field '%s' has incorrect outer link. MetaDescription '%s' has no Field '%s'", field.Name, field.LinkMeta.Name, field.Field.OuterLinkField),
				nil,
			)
		} else if !outerLinkField.canBeLinkTo(field.Meta) {
			return errors.NewValidationError(
				"new_meta",
				fmt.Sprintf("Field '%s' has incorrect outer link. FieldDescription '%s' of MetaDescription '%s' can't refer to MetaDescription '%s'", field.Name, outerLinkField.Name, outerLinkField.Meta.Name, field.Meta.Name),
				nil,
			)
		}
	}
	return nil
}

func NewMetaFactory(metaDriver MetaDescriptionSyncer) *MetaFactory {
	return &MetaFactory{make(map[string]*Meta, 0), metaDriver, make([]*Meta, 0)}
}
