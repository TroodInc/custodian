package object

import (
	"server/object/meta"
)

//Store
type Store struct {
	driver MetaDriver
	cache  *meta.MetaCache
}

//NewStore creates a Store object for specific underlying storage
func NewStore(driver MetaDriver) *Store {
	return &Store{driver: driver, cache: meta.NewCache()}
}

//TODO: implement
func (s *Store) NewMeta(metaObj *meta.Meta) (*meta.Meta, error) {
	return metaObj, nil
}



//List return full list of Meta objects from underlying storage
func (s *Store) List() []*meta.Meta {
	results, _ := s.driver.List()
	return results
}

//Get returns specific Meta object from underlying storage or cache
func (s *Store) Get(name string) *meta.Meta {
	if metaObj := s.cache.Get(name); metaObj != nil {
		// TODO: Must deal with "Dirty" objects
		return metaObj
	}

	metaObj := s.driver.Get(name)

	s.cache.Set(metaObj)

	return metaObj
}

//Saves Meta object to underlying storage resolving all related updates
func (s *Store) Create(m *meta.Meta) *meta.Meta {
	for _, field := range m.Fields {

		//TODO: Make FieldTypeObject & FieldTypeGeneric to use single field to store linked Meta objects
		if field.LinkType == meta.LinkTypeInner {
			outerField := &meta.Field{
				Name:           meta.ReverseInnerLinkName(m.Name),
				Type:           meta.FieldTypeArray,
				LinkType:       meta.LinkTypeOuter,
				LinkMeta:       m,
				OuterLinkField: field,
				Optional:       true,
				QueryMode:      true,
				RetrieveMode:   false,
			}
			switch field.Type {
			case meta.FieldTypeObject:
				field.LinkMeta.AddField(outerField)
			case meta.FieldTypeGeneric:
				for _, linkMeta := range field.LinkMetaList {
					linkMeta.AddField(outerField)
				}
			}
		}

	}

	if err := s.driver.Create(m); err == nil {
		for _, object := range s.cache.GetList() {
			if object.IsDirty() {
				s.driver.Update(object)
			}
		}
	}

	s.cache.Set(m)

	return m
}

func (s *Store) Update(m *meta.Meta) *meta.Meta {

	s.driver.Update(m)

	return m
}

//Remove Meta object from underlying storage resolving all related updates
func (s *Store) Remove(name string) {
	metaToRemove := s.Get(name)

	s.removeRelatedOuterLinks(metaToRemove)
	s.removeRelatedInnerLinks(metaToRemove)

	if _, err := s.driver.Remove(name); err == nil {

		for _, object := range s.cache.GetList() {
			if object.IsDirty() {
				s.driver.Update(object)
			}
		}
	}
}

//Flush removes all Meta objects fromunderlying storage
func (s *Store) Flush() {
	for _, m := range s.List() {
		s.Remove(m.Name)
	}
}



//Remove all outer fields linking to given MetaDescription
func (s *Store) removeRelatedOuterLinks(m *meta.Meta) {
	for _, field := range m.Fields {
		if field.Type == meta.FieldTypeObject && field.LinkType == meta.LinkTypeInner {
			s.removeRelatedOuterLink(m, field)
		}
	}
}

//Remove outer field from related object if it links to the given field
func (s *Store) removeRelatedOuterLink(m *meta.Meta, f *meta.Field) {
	relatedObject := f.LinkMeta
	for i, relatedObjectField := range relatedObject.Fields {
		if relatedObjectField.LinkType == meta.LinkTypeOuter &&
			relatedObjectField.LinkMeta.Name == m.Name &&
			relatedObjectField.OuterLinkField.Name == f.Name {
			//omit outer field and update related object
			// TODO: move to Meta.RemoveField() method
			delete(relatedObject.Fields, i)

			// TODO: Must be made by "Dirty" routine
			//s.Update(relatedMeta.Name, relatedMeta, true)
		}
	}
}

func (s *Store) removeRelatedInnerLinks(m *meta.Meta) {
	// TODO: reduce complexity by storing tree of related metas
	metaDescriptionList := s.List()
	for _, objectMetaDescription := range metaDescriptionList {
		if m.Name != objectMetaDescription.Name {
			if objectMeta := s.Get(objectMetaDescription.Name); objectMeta != nil {
				for i, field := range objectMeta.Fields {
					//omit orphan fields
					fieldIsTargetInnerLink :=
						field.LinkType == meta.LinkTypeInner && field.Type == meta.FieldTypeObject && field.LinkMeta.Name == m.Name

					fieldIsTargetOuterLink :=
						field.LinkType == meta.LinkTypeOuter && field.Type == meta.FieldTypeArray && field.LinkMeta.Name == m.Name

					if fieldIsTargetInnerLink || fieldIsTargetOuterLink {
						// TODO: Replace with Meta.RemoveField
						delete(objectMeta.Fields, i)
					}
					//else {
					//	objectNeedsUpdate = true
					//}
				}
				// TODO: Must be solved with "Dirty" routine
				// it means that related object should be updated
				//if objectNeedsUpdate {
				//	s.Update(objectMeta.Name, objectMeta, true)
				//}
			}
		}
	}
}

//
//Private stuff below
//

//func (s *Store) addReversedOuterFields(previousMeta *Meta, currentMeta *Meta) {
//	for _, field := range currentMeta.Fields {
//		if field.Type == FieldTypeObject && field.LinkType == LinkTypeInner {
//			var referencedMeta *Meta
//			if previousMeta != nil {
//				previousStateField := previousMeta.FindField(field.Name)
//				if previousStateField != nil {
//					//if field has already been added
//					//TODO: remove referencedMeta = field.LinkMeta outside of "if" block, this is a temporary workaround
//					//to provide backward compatibility with Topline schema
//					referencedMeta = field.LinkMeta
//					if previousStateField.LinkType != field.LinkType ||
//						previousStateField.Type != field.Type ||
//						previousStateField.LinkMeta.Name != field.LinkMeta.Name {
//						referencedMeta = field.LinkMeta
//					}
//				} else {
//					referencedMeta = field.LinkMeta
//				}
//			} else {
//				referencedMeta = field.LinkMeta
//			}
//
//			if referencedMeta != nil {
//				//add reverse outer
//				fieldName := ReverseInnerLinkName(currentMeta.Name)
//				if referencedMeta.FindField(fieldName) != nil {
//					continue
//				}
//				//automatically added outer field should only be available for querying
//				outerField := &Field{
//					Name:           fieldName,
//					Type:           FieldTypeArray,
//					LinkType:       LinkTypeOuter,
//					LinkMeta:       currentMeta,
//					OuterLinkField: field,
//					Optional:       true,
//					QueryMode:      true,
//					RetrieveMode:   false,
//				}
//				referencedMeta.Fields = append(
//					referencedMeta.Fields,
//					outerField,
//				)
//				metaStore.Update(referencedMeta.Name, referencedMeta, true)
//			}
//		}
//	}
//}
