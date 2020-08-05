package object

import "custodian/server/object/description"

func GetBaseMetaData(name string) *description.MetaDescription {
	return &description.MetaDescription{
		Name: name,
		Key:  "id",
		Cas:  false,
		Fields: []description.Field{
			{
				Name: "id",
				Type: description.FieldTypeNumber,
				Def: map[string]interface{}{
					"func": "nextval",
				},
			},
		},
	}
}
