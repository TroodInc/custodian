package description

import (
	"utils"
)

func GetBasicMetaDescription(nameOrRandom string) *MetaDescription {
	if nameOrRandom == "random" {
		//nameOrRandom = fmt.Sprintf("%x", time.Now().UnixNano())
		nameOrRandom = utils.RandomString(8)
	}

	return NewMetaDescription(
		nameOrRandom,
		"id",
		[]Field{
			{
				Name: "id",
				Type: FieldTypeNumber,
				Optional: true,
				Def: map[string]interface{}{
					"func": "nextval",
				},
			},
		},
		nil,
		false,
	)
}