package object

import (
	"custodian/server/errors"
	"custodian/server/object/description"
	"fmt"
	"sync"
)

type MetaCache struct {
	mutex    sync.RWMutex
	metaList map[string]*Meta
}

func (mc *MetaCache) Get(metaName string) *Meta {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()
	if meta, ok := mc.metaList[metaName]; ok {
		return meta
	} else {
		return nil
	}
}

func (mc *MetaCache) GetList() []*Meta {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()
	if len(mc.metaList) > 0 {
		metaList := make([]*Meta, 0)
		for _, meta := range mc.metaList {
			metaList = append(metaList, meta)
		}
		return metaList
	}
	return nil
}

func (mc *MetaCache) Set(meta *Meta) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	mc.metaList[meta.Name] = meta
}

func (mc *MetaCache) Invalidate() {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	mc.metaList = make(map[string]*Meta, 0)
}

func NewCache() *MetaCache {
	return &MetaCache{mutex: sync.RWMutex{}, metaList: make(map[string]*Meta, 0)}
}

func (mc *MetaCache) Delete(metaName string) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	delete(mc.metaList, metaName)
}

func (mc *MetaCache) Flush() {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	for metaName := range mc.metaList {
		delete(mc.metaList, metaName)
	}
}

func (mc *MetaCache) Fill(mdl []*description.MetaDescription) {
	for _, m := range mdl {
		meta := &Meta{MetaDescription: m}
		mc.Set(meta)
	}
	for _, m := range mc.GetList() {
		mc.resolveMeta(m)
	}
}

func (mc *MetaCache) FactoryMeta(m *description.MetaDescription) (*Meta, error) {
	if ok, err := (&description.MetaValidationService{}).Validate(m); !ok {
		return nil, err
	}
	metaObj := &Meta{MetaDescription: m}
	mc.Set(metaObj)

	err := mc.resolveMeta(metaObj)
	if err != nil {
		return nil, err
	}
	return metaObj, nil
}

func (mc *MetaCache) resolveMeta(currentMeta *Meta) error {
	//factory fields
	currentMeta.Fields = make([]FieldDescription, 0, len(currentMeta.MetaDescription.Fields))
	for _, field := range currentMeta.MetaDescription.Fields {
		fieldDescription, err := mc.factoryFieldDescription(field, currentMeta)
		if err != nil {
			return err
		}
		currentMeta.Fields = append(currentMeta.Fields, *fieldDescription)

	}

	mc.setLinks(currentMeta)

	//factory actionSet
	for i := range currentMeta.MetaDescription.Actions {
		//Setting id and Notifier for action
		// TODO action.id exists only in cache shold be unique id for each action
		if err := description.InitAction(i, &currentMeta.MetaDescription.Actions[i]); err == nil {
			currentMeta.Actions = append(currentMeta.Actions, &currentMeta.MetaDescription.Actions[i])
		}
	}

	currentMeta.Key = currentMeta.FindField(currentMeta.MetaDescription.Key)

	if err := checkOuterLinks(currentMeta); err != nil {
		return err
	}
	return nil

}

func (mc *MetaCache) factoryFieldDescription(field description.Field, objectMeta *Meta) (*FieldDescription, error) {
	fieldDescription := FieldDescription{
		Field:          &field,
		Meta:           objectMeta,
		LinkMeta:       nil,
		OuterLinkField: nil,
		LinkMetaList:   &MetaList{},
	}

	if field.LinkMeta != "" {
		if linkMeta, err := mc.buildMeta(field.LinkMeta); err != nil {
			return nil, err
		} else {
			fieldDescription.LinkMeta = linkMeta
		}

	}

	if field.Type == description.FieldTypeObjects && field.LinkType == description.LinkTypeInner {

		if linkThrough, err := mc.buildThroughMeta(&field, objectMeta); err != nil {
			return nil, err
		} else {
			fieldDescription.LinkThrough = linkThrough
		}

	}

	if len(field.LinkMetaList) > 0 {
		for _, metaName := range field.LinkMetaList {

			linkMeta, err := mc.buildMeta(metaName)
			if err != nil {
				return nil, errors.NewValidationError(
					"new_meta",
					fmt.Sprintf("Generic field references meta %s, which does not exist", metaName),
					nil,
				)
			}
			fieldDescription.LinkMetaList.AddMeta(linkMeta)
		}
	}
	return &fieldDescription, nil
}

func (mc *MetaCache) buildMeta(metaName string) (metaObj *Meta, err error) {
	if metaObj := mc.Get(metaName); metaObj != nil {
		return metaObj, nil
	}
	return nil, fmt.Errorf("%v not found in cahce", metaName)
}

//setLinks setes outer links and objects link
func (mc *MetaCache) setLinks(objectMeta *Meta) {
	for _, currentObjectMeta := range mc.GetList() {
		//processing outer links
		for i, _ := range currentObjectMeta.Fields {
			field := &currentObjectMeta.Fields[i]
			if field.LinkType == description.LinkTypeOuter {
				field.OuterLinkField = field.LinkMeta.FindField(field.Field.OuterLinkField)
			} else if field.Type == description.FieldTypeObjects {
				field.OuterLinkField = field.LinkThrough.FindField(field.Field.OuterLinkField)
			}
		}
	}
}

func (mc *MetaCache) buildThroughMeta(field *description.Field, ownerMeta *Meta) (*Meta, error) {
	metaName := fmt.Sprintf("%s__%s", ownerMeta.Name, field.LinkMeta)

	if meta := mc.Get(metaName); meta != nil {
		return meta, nil
	}

	fields := []description.Field{
		{
			Name: "id",
			Type: description.FieldTypeNumber,
			Def: map[string]interface{}{
				"func": "nextval",
			},
			Optional: true,
		},
		{
			Name:     ownerMeta.Name,
			Type:     description.FieldTypeObject,
			LinkMeta: ownerMeta.Name,
			LinkType: description.LinkTypeInner,
			Optional: false,
		},
		{
			Name:     field.LinkMeta,
			Type:     description.FieldTypeObject,
			LinkMeta: field.LinkMeta,
			LinkType: description.LinkTypeInner,
			Optional: false,
		},
	}
	//set outer link to the current field
	field.OuterLinkField = ownerMeta.Name
	metaDescription := description.NewMetaDescription(metaName, "id", fields, []description.Action{}, false)
	metaObj := &Meta{MetaDescription: metaDescription}

	mc.Set(metaObj)
	err := mc.resolveMeta(metaObj)
	if err != nil {
		return nil, err
	}
	return metaObj, nil
}
func checkOuterLinks(objectMeta *Meta) error {
	for i, _ := range objectMeta.Fields {
		field := &objectMeta.Fields[i]
		if field.LinkType != description.LinkTypeOuter {
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
