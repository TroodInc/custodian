package object

import (
	"server/object/meta"
	"utils"
)

func GetBaseMetaData(name string) *meta.Meta {
	return &meta.Meta{
		Name: name,
		Key:  "id",
		Cas:  false,
		Fields: map[string]*meta.Field{
			"id": {
				Name: "id",
				Type: meta.FieldTypeNumber,
				Def: map[string]interface{}{
					"func": "nextval",
				},
				Optional: true,
			},
		},
	}
}

//GetTwoBaseLinkedObjects creates B object with A inner link.
//
// A { id: int }  <--- B { id: int, a: innerObject }
func GetTwoBaseLinkedObjects(store *Store) (a, b *meta.Meta) {
	aMetaObj := GetBaseMetaData(utils.RandomString(8))
	a = store.Create(aMetaObj)

	bMetaObj := GetBaseMetaData(utils.RandomString(8))
	bMetaObj.AddField(&meta.Field{
		Name:     "a",
		Type:     meta.FieldTypeObject,
		LinkType: meta.LinkTypeInner,
		LinkMeta: aMetaObj,
	})
	b = store.Create(bMetaObj)

	return a, b
}