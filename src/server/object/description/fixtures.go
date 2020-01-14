package description

import (
	"fmt"
	"time"
)

func GetBasicMetaDescription(nameOrRandom string) *MetaDescription {
	if nameOrRandom == "random" {
		nameOrRandom = fmt.Sprintf("%x", time.Now().Unix())
	}

	return NewMetaDescription(
		nameOrRandom,
		"id",
		[]Field{
			{
				Name: "id",
				Type: FieldTypeNumber,
				Def: map[string]interface{}{
					"func": "nextval",
				},
			},
		},
		nil,
		false,
	)
}