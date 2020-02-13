package object

import "server/object/meta"

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
			},
		},
	}
}
