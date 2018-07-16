package meta

import (
	"sync"
	"io"
	"encoding/json"
	"logger"
	"utils"
)

/*
   Metadata store of objects persisted in DB.
*/
type MetaStore struct {
	drv         MetaDriver
	cache       map[string]*Meta
	cacheMutex  sync.RWMutex
	syncer      MetaDbSyncer
	syncerMutex sync.RWMutex
}

func (metaStore *MetaStore) UnmarshalJSON(r io.ReadCloser) (*Meta, error) {
	var metaObj MetaDescription
	if e := json.NewDecoder(r).Decode(&metaObj); e != nil {
		return nil, NewMetaError("", "unmarshal", ErrNotValid, e.Error())
	}
	return metaStore.NewMeta(&metaObj)
}

func (metaStore *MetaStore) unmarshalMeta(b []byte) (*Meta, error) {
	var m MetaDescription
	if e := json.Unmarshal(b, &m); e != nil {
		return nil, e
	}
	return metaStore.NewMeta(&m)
}

func (metaStore *MetaStore) NewMeta(metaObj *MetaDescription) (*Meta, error) {
	return NewMetaFactory(metaStore.drv).FactoryMeta(metaObj)
}

/*
   Retrives the list of metadata objects from the underlying store.
*/
func (metaStore *MetaStore) List() (*[]*MetaDescription, bool, error) {
	metaStore.syncerMutex.RLock()
	defer metaStore.syncerMutex.RUnlock()
	metaList, isFound, err := metaStore.drv.List()

	if err != nil {
		return &[]*MetaDescription{}, isFound, err
	}

	return metaList, isFound, nil
}

/*
   Retrives object metadata from the underlying store.
*/
func (metaStore *MetaStore) Get(name string, handleTransaction bool) (*Meta, bool, error) {
	if handleTransaction {
		metaStore.beginTransaction()
		defer metaStore.rollbackTransaction()
	}
	//retrieve business object metadata from the storage
	metaData, isFound, err := metaStore.drv.Get(name)

	if err != nil {
		return nil, isFound, err
	}
	//assemble the new business object with the given metadata
	businessObject, err := metaStore.NewMeta(metaData)
	if err != nil {
		return nil, isFound, err
	}
	//validate the newly created business object against existing one in the database
	ok, err := metaStore.syncer.ValidateObj(businessObject)
	if ok {
		return businessObject, isFound, nil
	}

	return nil, false, err
}

// Creates a new object type described by passed metadata.
func (metaStore *MetaStore) Create(m *Meta) error {
	//begin transaction
	metaStore.beginTransaction()
	defer metaStore.rollbackTransaction()

	if e := metaStore.syncer.CreateObj(m); e == nil {
		if e := metaStore.drv.Create(*m.MetaDescription); e == nil {
			metaStore.commitTransaction()
			return nil
		} else {
			var e2 = metaStore.syncer.RemoveObj(m.Name, false)
			logger.Error("Error while compenstaion of object '%s' metadata creation: %v", m.Name, e2)
			return e
		}
	} else {
		return e
	}
}

// Deletes an existing object metadata from the store.
func (metaStore *MetaStore) Remove(name string, force bool, handleTransaction bool) (bool, error) {
	if handleTransaction {
		//begin transaction
		metaStore.beginTransaction()
		defer metaStore.rollbackTransaction()
	}
	meta, _, err := metaStore.Get(name, false)
	if err != nil {
		return false, err
	}
	//remove related links from the database
	metaStore.removeRelatedOuterLinks(meta)
	metaStore.removeRelatedInnerLinks(meta)

	//remove related generic links from the database
	metaStore.removeRelatedGenericOuterLinks(meta)
	metaStore.removeRelatedGenericInnerLinks(meta)

	//remove object from the database
	if e := metaStore.syncer.RemoveObj(name, force); e == nil {
		//remove object`s description *.json file
		ok, err := metaStore.drv.Remove(name)
		if err == nil && handleTransaction {
			metaStore.commitTransaction()
		}
		return ok, err
	} else {
		return false, e
	}
}

//Remove all outer fields linking to given Meta
func (metaStore *MetaStore) removeRelatedOuterLinks(targetMeta *Meta) {
	for _, field := range targetMeta.Fields {
		if field.Type == FieldTypeObject && field.LinkType == LinkTypeInner {
			metaStore.removeRelatedOuterLink(targetMeta, field)
		}
	}
}

//Remove outer field from related object if it links to the given field
func (metaStore *MetaStore) removeRelatedOuterLink(targetMeta *Meta, innerLinkFieldDescription FieldDescription) {
	relatedObjectMeta := innerLinkFieldDescription.LinkMeta
	for i, relatedObjectField := range relatedObjectMeta.Fields {
		if relatedObjectField.LinkType == LinkTypeOuter &&
			relatedObjectField.LinkMeta.Name == targetMeta.Name &&
			relatedObjectField.OuterLinkField.Field.Name == innerLinkFieldDescription.Field.Name {
			//omit outer field and update related object
			relatedObjectMeta.Fields = append(relatedObjectMeta.Fields[:i], relatedObjectMeta.Fields[i+1:]...)
			relatedObjectMeta.MetaDescription.Fields = append(relatedObjectMeta.MetaDescription.Fields[:i], relatedObjectMeta.MetaDescription.Fields[i+1:]...)
			metaStore.Update(relatedObjectMeta.Name, relatedObjectMeta, false)
		}
	}
}

//Remove all outer fields linking to given Meta
func (metaStore *MetaStore) removeRelatedGenericOuterLinks(targetMeta *Meta) {
	for _, field := range targetMeta.Fields {
		if field.Type == FieldTypeGeneric && field.LinkType == LinkTypeInner {
			metaStore.removeRelatedToInnerGenericOuterLinks(targetMeta, field, field.LinkMetaList)
		}
	}
}

//Remove generic outer field from each of linkMetaList`s meta related to the given inner generic field
func (metaStore *MetaStore) removeRelatedToInnerGenericOuterLinks(targetMeta *Meta, genericInnerLinkFieldDescription FieldDescription, linkMetaList []*Meta) {
	for _, relatedObjectMeta := range linkMetaList {
		for i, relatedObjectField := range relatedObjectMeta.Fields {
			if relatedObjectField.Type == FieldTypeGeneric &&
				relatedObjectField.LinkType == LinkTypeOuter &&
				relatedObjectField.LinkMeta.Name == targetMeta.Name &&
				relatedObjectField.OuterLinkField.Field.Name == genericInnerLinkFieldDescription.Field.Name {
				//omit outer field and update related object
				relatedObjectMeta.Fields = append(relatedObjectMeta.Fields[:i], relatedObjectMeta.Fields[i+1:]...)
				relatedObjectMeta.MetaDescription.Fields = append(relatedObjectMeta.MetaDescription.Fields[:i], relatedObjectMeta.MetaDescription.Fields[i+1:]...)
				metaStore.Update(relatedObjectMeta.Name, relatedObjectMeta, false)
			}
		}
	}
}

//Remove inner fields linking to given Meta
func (metaStore *MetaStore) removeRelatedInnerLinks(targetMeta *Meta) {
	metaDescriptionList, _, _ := metaStore.List()
	for _, objectMetaDescription := range *metaDescriptionList {

		if targetMeta.Name != objectMetaDescription.Name {
			objectMeta, _, _ := metaStore.Get(objectMetaDescription.Name, false)
			objectMetaFields := make([]Field, 0)
			objectMetaFieldDescriptions := make([]FieldDescription, 0)
			objectNeedsUpdate := false

			for i, fieldDescription := range objectMeta.Fields {
				//omit orphan fields
				fieldIsTargetInnerLink := fieldDescription.LinkType == LinkTypeInner && fieldDescription.Type == FieldTypeObject && fieldDescription.LinkMeta.Name == targetMeta.Name
				fieldIsTargetOuterLink := fieldDescription.LinkType == LinkTypeOuter && fieldDescription.Type == FieldTypeArray && fieldDescription.LinkMeta.Name == targetMeta.Name

				if !(fieldIsTargetInnerLink || fieldIsTargetOuterLink) {
					objectMetaFields = append(objectMetaFields, objectMeta.MetaDescription.Fields[i])
					objectMetaFieldDescriptions = append(objectMetaFieldDescriptions, objectMeta.Fields[i])
				} else {
					objectNeedsUpdate = true
				}
			}
			// it means that related object should be updated
			if objectNeedsUpdate {
				objectMeta.Fields = objectMetaFieldDescriptions
				objectMeta.MetaDescription.Fields = objectMetaFields
				metaStore.Update(objectMeta.Name, objectMeta, false)
			}
		}
	}
}

//Remove inner fields linking to given Meta
func (metaStore *MetaStore) removeRelatedGenericInnerLinks(targetMeta *Meta) {
	metaDescriptionList, _, _ := metaStore.List()
	for _, objectMetaDescription := range *metaDescriptionList {

		if targetMeta.Name != objectMetaDescription.Name {
			objectMeta, _, _ := metaStore.Get(objectMetaDescription.Name, false)
			objectMetaFields := make([]Field, 0)
			objectMetaFieldDescriptions := make([]FieldDescription, 0)
			objectNeedsUpdate := false

			for i, fieldDescription := range objectMeta.Fields {
				//omit orphan fields
				fieldIsTargetGenericInnerLink := fieldDescription.LinkType == LinkTypeInner && fieldDescription.Type == FieldTypeGeneric && utils.Contains(fieldDescription.Field.LinkMetaList, targetMeta.Name)

				if !fieldIsTargetGenericInnerLink {
					objectMetaFields = append(objectMetaFields, objectMeta.MetaDescription.Fields[i])
					objectMetaFieldDescriptions = append(objectMetaFieldDescriptions, objectMeta.Fields[i])
				} else {
					objectNeedsUpdate = true
					if fieldIsTargetGenericInnerLink {
						indexOfTargetMeta := utils.IndexOf(fieldDescription.Field.LinkMetaList, targetMeta.Name)

						//alter field
						field := objectMeta.MetaDescription.Fields[i]
						field.LinkMetaList = append(field.LinkMetaList[:indexOfTargetMeta], field.LinkMetaList[indexOfTargetMeta+1:]...)
						objectMetaFields = append(objectMetaFields, field)

						//alter field description
						fieldDescription := objectMeta.Fields[i]
						fieldDescription.LinkMetaList = append(fieldDescription.LinkMetaList[:indexOfTargetMeta], fieldDescription.LinkMetaList[indexOfTargetMeta+1:]...)
						objectMetaFieldDescriptions = append(objectMetaFieldDescriptions, fieldDescription)
					}
				}
			}
			// it means that related object should be updated
			if objectNeedsUpdate {
				objectMeta.Fields = objectMetaFieldDescriptions
				objectMeta.MetaDescription.Fields = objectMetaFields
				metaStore.Update(objectMeta.Name, objectMeta, false)
			}
		}
	}
}

// Updates an existing object metadata.
func (metaStore *MetaStore) Update(name string, newBusinessObj *Meta, handleTransaction bool) (bool, error) {
	if handleTransaction {
		//begin transaction
		metaStore.beginTransaction()
		defer metaStore.rollbackTransaction()
	}

	if currentBusinessObj, ok, err := metaStore.Get(name, false); err == nil {
		// remove possible outer links before main update processing
		metaStore.processInnerLinksRemoval(currentBusinessObj, newBusinessObj)
		metaStore.processGenericInnerLinksRemoval(currentBusinessObj, newBusinessObj)

		ok, e := metaStore.drv.Update(name, *newBusinessObj.MetaDescription)

		if e != nil || !ok {
			return ok, e
		}

		//TODO: This logic tells NOTHING about error if it was successfully rolled back. This
		//behaviour should be fixed
		if updateError := metaStore.syncer.UpdateObj(currentBusinessObj, newBusinessObj); updateError == nil {
			if handleTransaction {
				metaStore.commitTransaction()
			}
			return true, nil
		} else {
			//rollback to the previous version
			rollbackError := metaStore.syncer.UpdateObjTo(currentBusinessObj)
			if rollbackError != nil {
				logger.Error("Error while rolling back an update of MetaDescription '%s': %v", name, rollbackError)
				return false, updateError

			}
			_, rollbackError = metaStore.drv.Update(name, *currentBusinessObj.MetaDescription)
			if rollbackError != nil {
				logger.Error("Error while rolling back an update of MetaDescription '%s': %v", name, rollbackError)
				return false, updateError
			}
			return false, updateError
		}
	} else {
		return ok, err
	}
}

// compare current object`s version to the version is being updated and remove outer links
// if any inner link is being removed
func (metaStore *MetaStore) processInnerLinksRemoval(currentMeta *Meta, metaToBeUpdated *Meta) {
	for _, currentFieldDescription := range currentMeta.Fields {
		if currentFieldDescription.LinkType == LinkTypeInner && currentFieldDescription.Type == FieldTypeObject {
			fieldIsBeingRemoved := true
			for _, fieldDescriptionToBeUpdated := range metaToBeUpdated.Fields {
				if fieldDescriptionToBeUpdated.Name == fieldDescriptionToBeUpdated.Name &&
					fieldDescriptionToBeUpdated.LinkType == LinkTypeInner &&
					fieldDescriptionToBeUpdated.LinkMeta.Name == fieldDescriptionToBeUpdated.LinkMeta.Name {
					fieldIsBeingRemoved = false
				}
			}
			if fieldIsBeingRemoved {
				metaStore.removeRelatedOuterLink(currentMeta, currentFieldDescription)
			}
		}
	}
}

// compare current object`s version to the version is being updated and remove outer links
// if any generic inner link is being removed
func (metaStore *MetaStore) processGenericInnerLinksRemoval(currentMeta *Meta, metaToBeUpdated *Meta) {
	for _, currentFieldDescription := range currentMeta.Fields {
		if currentFieldDescription.LinkType == LinkTypeInner && currentFieldDescription.Type == FieldTypeGeneric {
			fieldIsBeingRemoved := true
			fieldIsBeingUpdated := true
			var linkMetaListDiff []*Meta
			for _, fieldDescriptionToBeUpdated := range metaToBeUpdated.Fields {
				if fieldDescriptionToBeUpdated.Name == currentFieldDescription.Name &&
					fieldDescriptionToBeUpdated.LinkType == LinkTypeInner {
					if utils.Equal(fieldDescriptionToBeUpdated.Field.LinkMetaList, currentFieldDescription.Field.LinkMetaList) {
						fieldIsBeingRemoved = false
						fieldIsBeingUpdated = false
					} else {
						fieldIsBeingRemoved = false
						fieldIsBeingUpdated = true
						linkMetaListDiff = MetaListDiff(currentFieldDescription.LinkMetaList, fieldDescriptionToBeUpdated.LinkMetaList)
					}
				}
			}
			//process generic outer link removal only for removed metas
			if fieldIsBeingUpdated {
				metaStore.removeRelatedToInnerGenericOuterLinks(currentMeta, currentFieldDescription, linkMetaListDiff)
			}
			//process generic outer link removal for each linked meta
			if fieldIsBeingRemoved {
				metaStore.removeRelatedToInnerGenericOuterLinks(currentMeta, currentFieldDescription, currentFieldDescription.LinkMetaList)
			}
		}
	}
}

// Updates an existing object metadata.
func (metaStore *MetaStore) Flush() error {
	metaStore.beginTransaction()
	defer metaStore.rollbackTransaction()
	metaList, _, err := metaStore.List()
	if err != nil {
		return err
	}
	for _, meta := range *metaList {
		if _, err := metaStore.Remove(meta.Name, true, false); err != nil {
			return err
		}
	}
	metaStore.commitTransaction()
	return nil
}

func (metaStore *MetaStore) beginTransaction() (error) {
	if err := metaStore.syncer.BeginTransaction(); err != nil {
		return err
	}
	if err := metaStore.drv.BeginTransaction(); err != nil {
		return err
	}
	return nil
}

func (metaStore *MetaStore) commitTransaction() (error) {
	if err := metaStore.syncer.CommitTransaction(); err != nil {
		return err
	}
	if err := metaStore.drv.CommitTransaction(); err != nil {
		return err
	}
	return nil
}

func (metaStore *MetaStore) rollbackTransaction() {
	metaStore.syncer.RollbackTransaction()
	metaStore.drv.RollbackTransaction()
}

func NewStore(md MetaDriver, mds MetaDbSyncer) *MetaStore {
	return &MetaStore{drv: md, syncer: mds, cache: make(map[string]*Meta)}
}
