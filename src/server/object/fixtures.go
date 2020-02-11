package object

func GetBaseMetaData(name string) *Meta {
	return &Meta{
		Name: name,
		Key:  "id",
		Cas:  false,
		Fields: []*Field{
			{
				Name: "id",
				Type: FieldTypeNumber,
				Def: map[string]interface{}{
					"func": "nextval",
				},
			},
		},
	}
}
