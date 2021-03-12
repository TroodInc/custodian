package meta

import (
	"custodian/logger"
	"custodian/server/errors"
	. "custodian/server/object/description"
	"custodian/server/transactions"
	"custodian/utils"
	"encoding/json"
	"io"
	"strings"
)

/*
   Metadata store of objects persisted in DB.
*/
type MetaStore struct {
	MetaDescriptionSyncer    MetaDescriptionSyncer
	Syncer                   MetaDbSyncer
	globalTransactionManager *transactions.GlobalTransactionManager
}

func (metaStore *MetaStore) UnmarshalIncomingJSON(r io.Reader) (*Meta, error) {
	var metaObj MetaDescription
	if e := json.NewDecoder(r).Decode(&metaObj); e != nil {
		return nil, errors.NewFatalError(ErrNotValid, "unmarshal", e.Error())
	}
	// normalize description
	return metaStore.NewMeta((&NormalizationService{}).Normalize(&metaObj))
}

func (metaStore *MetaStore) NewMeta(metaObj *MetaDescription) (*Meta, error) {
	return NewMetaFactory(metaStore.MetaDescriptionSyncer).FactoryMeta(metaObj)
}

/*
   Gets the list of metadata objects from the underlying store.
*/
func (metaStore *MetaStore) List() ([]*MetaDescription, bool, error) {
	metaList, isFound, err := metaStore.MetaDescriptionSyncer.List()

	if err != nil {
		return []*MetaDescription{}, isFound, err
	}

	return metaList, isFound, nil
}

/*
   Retrieves object metadata from the underlying store.
*/
func (metaStore *MetaStore) Get(name string, useCache bool) (*Meta, bool, error) {
	//try to get meta from cache
	if useCache {
		metaObj := metaStore.MetaDescriptionSyncer.Cache().Get(name)
		if metaObj != nil {
			return metaObj, true, nil
		}
	}

	//retrieve business object metadata from the storage
	metaData, isFound, err := metaStore.MetaDescriptionSyncer.Get(name)

	if err != nil {
		return nil, isFound, err
	}
	//assemble the new business object with the given metadata
	metaObj, err := metaStore.NewMeta(metaData)
	if err != nil {
		return nil, isFound, err
	}

	return metaObj, isFound, nil

	// @todo: Find another way of checking DB schema consistency
	//transaction, _ := metaStore.globalTransactionManager.BeginTransaction(nil)
	//validate the newly created business object against existing one in the database
	//ok, err := metaStore.Syncer.ValidateObj(transaction.DbTransaction, metaObj.MetaDescription, metaStore.MetaDescriptionSyncer)
	//if ok {
	//	metaStore.cache.Set(metaObj)
	//	metaStore.globalTransactionManager.CommitTransaction(transaction)
	//	return metaObj, isFound, nil
	//}
	//metaStore.globalTransactionManager.RollbackTransaction(transaction)
	//return nil, false, err
}

// Creates a new object type described by passed metadata.
func (metaStore *MetaStore) Create(objectMeta *Meta) error {
	if e := metaStore.Syncer.CreateObj(metaStore.globalTransactionManager, objectMeta.MetaDescription, metaStore.MetaDescriptionSyncer); e == nil {
		if e := metaStore.MetaDescriptionSyncer.Create(*objectMeta.MetaDescription); e == nil {

			//add corresponding outer generic fields
			metaStore.addReversedOuterGenericFields(nil, objectMeta)
			//add corresponding outer field
			metaStore.addReversedOuterFields(nil, objectMeta)
			//create through object if needed
			if err := metaStore.createThroughMeta(objectMeta); err != nil {
				return err
			}

			//invalidate cache
			metaStore.MetaDescriptionSyncer.Cache().Invalidate()
			return nil
		} else {
			var e2 = metaStore.Syncer.RemoveObj(metaStore.globalTransactionManager, objectMeta.Name, false)
			logger.Error("Error while compenstaion of object '%s' metadata creation: %v", objectMeta.Name, e2)
			return e
		}
	} else {
		return e
	}
}

// Updates an existing object metadata.
func (metaStore *MetaStore) Update(name string, newMetaObj *Meta, keepOuter bool, upateRelated bool) (bool, error) {
	if currentMetaObj, ok, err := metaStore.Get(name, false); err == nil {

		if keepOuter {
			metaStore.processGenericOuterLinkKeeping(currentMetaObj, newMetaObj)
		}
		// remove possible outer links before main update processing
		metaStore.removeRelatedLinksOnUpdate(upateRelated, currentMetaObj, newMetaObj)

		if updateError := metaStore.Syncer.UpdateObj(metaStore.globalTransactionManager, currentMetaObj.MetaDescription, newMetaObj.MetaDescription, metaStore.MetaDescriptionSyncer); updateError == nil {
			//add corresponding outer generic fields
			metaStore.addReversedOuterGenericFields(currentMetaObj, newMetaObj)

			ok, e := metaStore.MetaDescriptionSyncer.Update(name, *newMetaObj.MetaDescription)
			if e != nil || !ok {
				return ok, e
			}

			//add corresponding outer field
			metaStore.addReversedOuterFields(currentMetaObj, newMetaObj)

			//create through object if needed
			if err := metaStore.createThroughMeta(newMetaObj); err != nil {
				return false, err
			}

			//invalidate cache
			metaStore.MetaDescriptionSyncer.Cache().Invalidate()
			return true, nil
		} else {
			return false, updateError
		}
	} else {
		return ok, err
	}
}

// Deletes an existing object metadata from the store.
func (metaStore *MetaStore) Remove(name string, force bool) (bool, error) {
	meta, _, err := metaStore.Get(name, false)
	if err != nil {
		return false, err
	}
	//remove related links from the database
	metaStore.removeRelatedLinks(force, meta)

	//remove object from the database
	if e := metaStore.Syncer.RemoveObj(metaStore.globalTransactionManager, name, force); e == nil {
		//remove object`s description *.json file
		ok, err := metaStore.MetaDescriptionSyncer.Remove(name)

		//invalidate cache
		metaStore.MetaDescriptionSyncer.Cache().Invalidate()

		return ok, err
	} else {
		return false, e
	}
}

// removeRelatedLinks removes all related links
// keepMeta should be false to remove intermediate m2m table
// targetMeta - meta of object that is about to be deleted
// first related "object" link is removed than all other related links are removed
func (metaStore *MetaStore) removeRelatedLinks(keepMeta bool, targetMeta *Meta) error {
	// remove related object fields
	metaDescriptionList, _, _ := metaStore.List()
	for _, objectMeta := range metaDescriptionList {
		// do not take m2m intermediate tables
		if !strings.Contains(objectMeta.Name, "__") {
			metaStore.updateRelatedObj(keepMeta, objectMeta.Name, targetMeta.Name, FieldTypeObject)
		}
	}

	for _, field := range targetMeta.Fields {
		metaStore.processRelatedObjectUpdate(keepMeta, field, targetMeta.Name)
	}
	return nil
}

// removeRelatedLinksOnUpdate removes related links on object update
// if updateRelated is false no update actions on related links will occur
func (metaStore *MetaStore) removeRelatedLinksOnUpdate(updateRelated bool, currentMeta *Meta, newMeta *Meta) error {
	if updateRelated {
		for _, currentFieldDescription := range currentMeta.Fields {
			isInnerLink := currentFieldDescription.LinkType == LinkTypeInner && currentFieldDescription.Type == FieldTypeObject
			isArrayLink := currentFieldDescription.LinkType == LinkTypeOuter && currentFieldDescription.Type == FieldTypeArray
			isObjectsLink := currentFieldDescription.LinkType == LinkTypeInner && currentFieldDescription.Type == FieldTypeObjects
			isInnerGeneric := currentFieldDescription.LinkType == LinkTypeInner && currentFieldDescription.Type == FieldTypeGeneric
			isOuterGeneric := currentFieldDescription.LinkType == LinkTypeOuter && currentFieldDescription.Type == FieldTypeGeneric
			if isInnerLink || isArrayLink || isObjectsLink || isInnerGeneric || isOuterGeneric {
				if newField := newMeta.FindField(currentFieldDescription.Name); newField == nil {
					if !isArrayLink && !isOuterGeneric {
						metaStore.processRelatedObjectUpdate(false, currentFieldDescription, currentMeta.Name)
					} else if isOuterGeneric {
						var isRenamed bool
						for _, f := range newMeta.Fields {
							if f.LinkMeta != nil && currentFieldDescription.LinkMeta.Name == f.LinkMeta.Name &&
								f.Type == FieldTypeGeneric && f.LinkType == LinkTypeOuter {
								isRenamed = true
							}
						}
						if !isRenamed {
							metaStore.processRelatedObjectUpdate(false, currentFieldDescription, currentMeta.Name)
						}
					}

				} else if newField != nil {

					var sameLinkMeta bool
					sameName := currentFieldDescription.Name == newField.Name
					sameType := currentFieldDescription.Type == newField.Type

					if currentFieldDescription.LinkMeta != nil {
						if newField.LinkMeta == nil && currentFieldDescription.LinkMeta == nil {
							sameLinkMeta = true
						} else if newField.LinkMeta != nil && currentFieldDescription.LinkMeta != nil {
							sameLinkMeta = currentFieldDescription.LinkMeta.Name == newField.LinkMeta.Name
						}
					}

					isChanged := !sameName || !sameType || !sameLinkMeta

					if isChanged && (isInnerLink || isArrayLink) {
						metaStore.updateRelatedObj(true, currentFieldDescription.LinkMeta.Name, currentMeta.Name, FieldTypeArray)

					} else if isChanged && isObjectsLink {
						if _, err := metaStore.Remove(currentFieldDescription.LinkThrough.Name, true); err != nil {
							return err
						}

					} else if isChanged && isInnerGeneric {
						sameLinkMetaList := utils.Equal(currentFieldDescription.Field.LinkMetaList, newField.Field.LinkMetaList, false)
						if !sameLinkMetaList {
							linkMetaListDiff := currentFieldDescription.LinkMetaList.Diff(newField.LinkMetaList.GetAll())
							for _, linkMeta := range linkMetaListDiff {
								metaStore.updateRelatedObj(true, linkMeta.Name, currentMeta.Name, FieldTypeGeneric)
							}
						}
					} else if isChanged && isOuterGeneric {
						metaStore.updateRelatedObj(true, currentFieldDescription.LinkMeta.Name, currentMeta.Name, FieldTypeGeneric)
					}

				}
			}
		}
	}
	return nil
}

// processRelatedObjectUpdate is used to update related object and remove linked field from its meta
// field is a field which is about to be deleted from linked object
// targetMetaName name of object that is updating or removing (action on target object cause the related object update)
func (metaStore *MetaStore) processRelatedObjectUpdate(keepMeta bool, field FieldDescription, targetMetaName string) error {
	switch field.Type {
	// remove related intermediate table a__b
	case FieldTypeObjects:
		if !keepMeta {
			if _, err := metaStore.Remove(field.LinkThrough.Name, true); err != nil {
				return err
			}
		}
	// remove related objects filed (m2m)
	case FieldTypeArray:
		// process a__b_set field remove
		if strings.Contains(field.LinkMeta.Name, "__") {
			m2mIntermediateTable, _, _ := metaStore.Get(field.LinkMeta.Name, false)
			if m2mIntermediateTable != nil {
				var relatedObjName string
				// find field of "objects" type
				for _, f := range m2mIntermediateTable.Fields {
					if f.Type == FieldTypeObject && f.Name != targetMetaName {
						relatedObjName = f.Name
					}
				}
				// should first remove a__b_set field in related object
				metaStore.updateRelatedObj(keepMeta, relatedObjName, field.LinkMeta.Name, FieldTypeArray)
				metaStore.updateRelatedObj(keepMeta, relatedObjName, targetMetaName, FieldTypeObjects)
			}
		}

	// remove related outer field
	case FieldTypeObject:
		metaStore.updateRelatedObj(keepMeta, field.LinkMeta.Name, targetMetaName, FieldTypeArray)
	case FieldTypeGeneric:
		if field.LinkType == LinkTypeInner {
			for _, linkedMeta := range field.LinkMetaList.GetAll() {
				metaStore.updateRelatedObj(keepMeta, linkedMeta.Name, targetMetaName, FieldTypeGeneric)
			}
		} else if field.LinkType == LinkTypeOuter {

			metaStore.updateRelatedObj(keepMeta, field.LinkMeta.Name, targetMetaName, FieldTypeGeneric)
		}
	}
	return nil
}

// updateRelatedObj is used to update related object
// keepMeta should be false to remove m2m intermediate table
// relatedObjectName is a name of related object that is about to be updated
// targetMetaName name of object that is updating or removing (action on target object cause the related object update)
// relatedFieldType is the type of field that is about to be removed
func (metaStore *MetaStore) updateRelatedObj(keepMeta bool, relatedObjectName string, targetMetaName string, relatedFieldType FieldType) error {
	objectMeta, _, _ := metaStore.Get(relatedObjectName, false)
	if objectMeta != nil {
		objectMetaFields := make([]Field, 0)
		objectMetaFieldDescriptions := make([]FieldDescription, 0)
		objectNeedsUpdate := false
		for i, fieldDescription := range objectMeta.Fields {

			fieldIsObjectsLink := fieldDescription.LinkType == LinkTypeInner && fieldDescription.Type == FieldTypeObjects && fieldDescription.LinkMeta.Name == targetMetaName
			fieldIsTargetOuterLink := fieldDescription.LinkType == LinkTypeOuter && fieldDescription.Type == FieldTypeArray && fieldDescription.LinkMeta.Name == targetMetaName
			fieldIsTargetInnerLink := fieldDescription.LinkType == LinkTypeInner && fieldDescription.Type == FieldTypeObject && fieldDescription.LinkMeta.Name == targetMetaName
			fieldIsTargetGenericInnerLink := fieldDescription.LinkType == LinkTypeInner && fieldDescription.Type == FieldTypeGeneric && utils.Contains(fieldDescription.Field.LinkMetaList, targetMetaName)
			fieldIsTargetGenericOuterLink := fieldDescription.LinkType == LinkTypeOuter && fieldDescription.Type == FieldTypeGeneric && fieldDescription.LinkMeta.Name == targetMetaName

			if !(fieldIsTargetInnerLink || fieldIsTargetOuterLink || fieldIsObjectsLink || fieldIsTargetGenericInnerLink || fieldIsTargetGenericOuterLink) {
				objectMetaFields = append(objectMetaFields, objectMeta.MetaDescription.Fields[i])
				objectMetaFieldDescriptions = append(objectMetaFieldDescriptions, objectMeta.Fields[i])

			} else if fieldIsTargetGenericInnerLink && fieldDescription.Type == relatedFieldType {
				objectNeedsUpdate = true

				indexOfTargetMeta := utils.IndexOf(fieldDescription.Field.LinkMetaList, targetMetaName)

				//alter field
				field := objectMeta.MetaDescription.Fields[i]
				field.LinkMetaList = append(field.LinkMetaList[:indexOfTargetMeta], field.LinkMetaList[indexOfTargetMeta+1:]...)
				objectMetaFields = append(objectMetaFields, field)

				//alter field description
				fieldDescription := objectMeta.Fields[i]
				fieldDescription.LinkMetaList.RemoveByName(targetMetaName)
				objectMetaFieldDescriptions = append(objectMetaFieldDescriptions, fieldDescription)

			} else if (fieldIsTargetInnerLink || fieldIsTargetOuterLink || fieldIsTargetGenericOuterLink) && fieldDescription.Type == relatedFieldType {
				objectNeedsUpdate = true
			} else if fieldIsObjectsLink && fieldDescription.Type == relatedFieldType {
				objectNeedsUpdate = true
				if !keepMeta {
					if _, err := metaStore.Remove(fieldDescription.LinkThrough.Name, true); err != nil {
						return err
					}
				}
			}
		}

		if objectNeedsUpdate {
			objectMeta.Fields = objectMetaFieldDescriptions
			objectMeta.MetaDescription.Fields = objectMetaFields
			if _, err := metaStore.Update(objectMeta.Name, objectMeta, false, false); err != nil {
				return err
			}
		}
	}
	return nil
}

//track outer link removal and return it if corresponding inner generic field was not removed
//do nothing if field is being renamed
func (metaStore *MetaStore) processGenericOuterLinkKeeping(previousMeta *Meta, currentMeta *Meta) {
	for _, previousMetaField := range previousMeta.Fields {
		if (previousMetaField.Type == FieldTypeGeneric || previousMetaField.Type == FieldTypeArray) &&
			previousMetaField.LinkType == LinkTypeOuter &&
			currentMeta.FindField(previousMetaField.Name) == nil {

			if _, _, err := metaStore.Get(previousMetaField.LinkMeta.Name, false); err == nil {
				fieldIsBeingRenamed := false
				for _, currentMetaField := range currentMeta.Fields {
					if (currentMetaField.Type == FieldTypeGeneric || currentMetaField.Type == FieldTypeArray) &&
						currentMetaField.LinkType == LinkTypeOuter &&
						currentMetaField.LinkMeta.Name == previousMetaField.LinkMeta.Name {
						fieldIsBeingRenamed = true
					}
				}

				if !fieldIsBeingRenamed {
					previousMetaField.RetrieveMode = false
					previousMetaField.QueryMode = true
					currentMeta.AddField(previousMetaField)
				}
			}
		}
	}
}

//add corresponding reverse outer generic field if field is added
func (metaStore *MetaStore) addReversedOuterGenericFields(previousMeta *Meta, currentMeta *Meta) {
	for _, field := range currentMeta.Fields {
		if field.Type == FieldTypeGeneric && field.LinkType == LinkTypeInner {
			shouldProcessOuterLinks := true
			var excludedMetas []*Meta
			var includedMetas []*Meta

			if previousMeta != nil {
				previousStateField := previousMeta.FindField(field.Name)
				if previousStateField != nil {
					//if field has already been added
					if previousStateField.LinkType == field.LinkType && previousStateField.Type == field.Type {
						shouldProcessOuterLinks = false
						if excludedMetas = previousStateField.LinkMetaList.Diff(field.LinkMetaList.metas); len(excludedMetas) > 0 {
							shouldProcessOuterLinks = true
						}
						if includedMetas = field.LinkMetaList.Diff(previousStateField.LinkMetaList.metas); len(includedMetas) > 0 {
							shouldProcessOuterLinks = true
						}
					}
				} else {
					includedMetas = field.LinkMetaList.metas
				}
			} else {
				includedMetas = field.LinkMetaList.metas
			}

			if shouldProcessOuterLinks {
				//add reverse outer
				for _, linkMeta := range includedMetas {
					fieldName := ReverseInnerLinkName(currentMeta.Name)
					if linkMeta.FindField(fieldName) != nil {
						continue
					}
					outerField := Field{
						Name:           fieldName,
						Type:           FieldTypeGeneric,
						LinkType:       LinkTypeOuter,
						LinkMeta:       currentMeta.Name,
						OuterLinkField: field.Name,
						Optional:       true,
						QueryMode:      true,
						RetrieveMode:   false,
					}
					linkMeta.MetaDescription.Fields = append(
						linkMeta.MetaDescription.Fields,
						outerField,
					)
					linkMeta.Fields = append(linkMeta.Fields,
						FieldDescription{
							Field:          &outerField,
							Meta:           linkMeta,
							LinkMeta:       currentMeta,
							OuterLinkField: &field},
					)
					metaStore.Update(linkMeta.Name, linkMeta, true, false)
				}

				//remove reverse outer
				for _, excludedMeta := range excludedMetas {
					for i, excludedField := range excludedMeta.Fields {
						if excludedField.Type == FieldTypeGeneric && excludedField.LinkType == LinkTypeOuter {
							if excludedField.OuterLinkField.Name == field.Name && excludedField.LinkMeta.Name == field.Meta.Name {
								excludedMeta.Fields = append(excludedMeta.Fields[:i], excludedMeta.Fields[i+1:]...)
								excludedMeta.MetaDescription.Fields = append(excludedMeta.MetaDescription.Fields[:i], excludedMeta.MetaDescription.Fields[i+1:]...)
								metaStore.Update(excludedMeta.Name, excludedMeta, true, false)
							}
						}
					}
				}
			}
		}
	}
}

//add corresponding reverse outer generic field if field is added
func (metaStore *MetaStore) addReversedOuterFields(previousMeta *Meta, currentMeta *Meta) {
	for _, field := range currentMeta.Fields {
		if field.Type == FieldTypeObject && field.LinkType == LinkTypeInner {
			var referencedMeta *Meta
			if previousMeta != nil {
				previousStateField := previousMeta.FindField(field.Name)
				if previousStateField != nil {
					//if field has already been added
					//TODO: remove referencedMeta = field.LinkMeta outside of "if" block, this is a temporary workaround
					//to provide backward compatibility with Topline schema
					referencedMeta = field.LinkMeta
					if previousStateField.LinkType != field.LinkType ||
						previousStateField.Type != field.Type ||
						previousStateField.LinkMeta.Name != field.LinkMeta.Name {
						referencedMeta = field.LinkMeta
					}
				} else {
					referencedMeta = field.LinkMeta
				}
			} else {
				referencedMeta = field.LinkMeta
			}

			if referencedMeta != nil {
				//add reverse outer
				fieldName := ReverseInnerLinkName(currentMeta.Name)
				if referencedMeta.FindField(fieldName) != nil {
					continue
				}
				//automatically added outer field should only be available for querying
				outerField := Field{
					Name:           fieldName,
					Type:           FieldTypeArray,
					LinkType:       LinkTypeOuter,
					LinkMeta:       currentMeta.Name,
					OuterLinkField: field.Name,
					Optional:       true,
					QueryMode:      true,
					RetrieveMode:   false,
				}
				referencedMeta.MetaDescription.Fields = append(
					referencedMeta.MetaDescription.Fields,
					outerField,
				)
				referencedMeta.Fields = append(referencedMeta.Fields,
					FieldDescription{
						Field:          &outerField,
						Meta:           referencedMeta,
						LinkMeta:       currentMeta,
						OuterLinkField: &field,
					},
				)
				metaStore.Update(referencedMeta.Name, referencedMeta, true, false)
			}
		}
	}
}

//create through meta if it does not exist
func (metaStore *MetaStore) createThroughMeta(meta *Meta) error {
	for _, field := range meta.Fields {
		if field.Type == FieldTypeObjects {
			if _, _, err := metaStore.Get(field.LinkThrough.Name, false); err != nil {
				return metaStore.Create(field.LinkThrough)
			}
		}
	}
	return nil
}

// Updates an existing object metadata.
func (metaStore *MetaStore) Flush() error {
	metaList, _, err := metaStore.List()
	if err != nil {
		return err
	}
	for _, meta := range metaList {
		// check object exists
		if meta, _, _ := metaStore.Get(meta.Name, false); meta != nil {
			if _, err := metaStore.Remove(meta.Name, false); err != nil {
				return err
			}
		}

	}
	return nil
}

func NewStore(md MetaDescriptionSyncer, mds MetaDbSyncer, gtm *transactions.GlobalTransactionManager) *MetaStore {
	return &MetaStore{MetaDescriptionSyncer: md, Syncer: mds, globalTransactionManager: gtm}
}
