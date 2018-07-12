package pg

import (
	"server/meta"
	"fmt"
)

type MetaDdlFactory struct{}

func (metaDdlFactory *MetaDdlFactory) Factory(m *meta.Meta) (*MetaDDL, error) {
	var metaDdl = &MetaDDL{Table: tblName(m), Pk: m.Key.Name}
	metaDdl.Columns = make([]Column, 0, )
	metaDdl.IFKs = make([]IFK, 0)
	metaDdl.OFKs = make([]OFK, 0)
	metaDdl.Seqs = make([]Seq, 0)
	for _, field := range m.Fields {
		if columns, ifk, ofk, seq, err := metaDdlFactory.processField(&field); err != nil {
			return nil, err
		} else {
			metaDdl.Columns = append(metaDdl.Columns, columns...)
			if ifk != nil {
				metaDdl.IFKs = append(metaDdl.IFKs, *ifk)
			}
			if ofk != nil {
				metaDdl.OFKs = append(metaDdl.OFKs, *ofk)
			}
			if seq != nil {
				metaDdl.Seqs = append(metaDdl.Seqs, *seq)
			}
		}
	}
	return metaDdl, nil
}

func (metaDdlFactory *MetaDdlFactory) processField(field *meta.FieldDescription) ([]Column, *IFK, *OFK, *Seq, error) {
	if field.IsSimple() {
		return metaDdlFactory.processSimpleField(field)
	} else if field.Type == meta.FieldTypeObject && field.LinkType == meta.LinkTypeInner {
		return metaDdlFactory.processInnerLinkField(field)
	} else if field.LinkType == meta.LinkTypeOuter {
		return metaDdlFactory.processOuterLinkField(field)
	} else {
		return nil, nil, nil, nil, &DDLError{table: field.Meta.Name, code: ErrUnsupportedLinkType, msg: fmt.Sprintf("Unsupported link type lt = %v, ft = %v", string(field.LinkType), string(field.LinkType))}

	}

	return nil, nil, nil, nil, nil
}

func (metaDdlFactory *MetaDdlFactory) processSimpleField(field *meta.FieldDescription) ([]Column, *IFK, *OFK, *Seq, error) {
	column, err := metaDdlFactory.factoryBlankColumn(field)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	var ok bool
	if column.Typ, ok = fieldTypeToColumnType(field.Type); !ok {
		return nil, nil, nil, nil, &DDLError{table: field.Meta.Name, code: ErrUnsupportedColumnType, msg: "Unsupported field type: " + string(field.Type)}
	}
	return []Column{*column}, nil, nil, metaDdlFactory.factorySequence(field), nil
}

func (metaDdlFactory *MetaDdlFactory) processInnerLinkField(field *meta.FieldDescription) ([]Column, *IFK, *OFK, *Seq, error) {
	column, err := metaDdlFactory.factoryBlankColumn(field)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	var ok bool
	if column.Typ, ok = fieldTypeToColumnType(field.LinkMeta.Key.Type); !ok {
		return nil, nil, nil, nil, &DDLError{table: field.Meta.Name, code: ErrUnsupportedColumnType, msg: "Unsupported field type: " + string(field.LinkMeta.Key.Type)}
	}

	ifk := IFK{FromColumn: field.Name, ToTable: tblName(field.LinkMeta), ToColumn: field.LinkMeta.Key.Name}

	return []Column{*column}, &ifk, nil, metaDdlFactory.factorySequence(field), nil
}

func (metaDdlFactory *MetaDdlFactory) processOuterLinkField(field *meta.FieldDescription) ([]Column, *IFK, *OFK, *Seq, error) {
	outerForeignKey := OFK{FromTable: tblName(field.LinkMeta), FromColumn: field.OuterLinkField.Name, ToTable: tblName(field.Meta), ToColumn: field.Meta.Key.Name}
	return nil, nil, &outerForeignKey, nil, nil

}

// factory common column
func (metaDdlFactory *MetaDdlFactory) factoryBlankColumn(field *meta.FieldDescription) (*Column, error) {
	column := Column{}
	column.Name = field.Name
	column.Optional = field.Optional
	column.Unique = false

	var err error
	column.Defval, err = metaDdlFactory.getDefaultValue(field)
	if column.Defval, err = metaDdlFactory.getDefaultValue(field); err != nil {
		return nil, err
	}

	return &column, nil
}

// factory default value sequence
func (metaDdlFactory *MetaDdlFactory) factorySequence(field *meta.FieldDescription) *Seq {
	colDef, err := newColDefVal(field)
	if err != nil {
		return nil
	}
	var sequence *Seq
	if columnDefaultValueSequence, _ := colDef.(*ColDefValSeq); columnDefaultValueSequence != nil {
		sequence = columnDefaultValueSequence.seq
	}
	return sequence
}

//get default value
func (metaDdlFactory *MetaDdlFactory) getDefaultValue(field *meta.FieldDescription) (string, error) {
	colDef, err := newColDefVal(field)
	if err != nil {
		return "", err
	}
	return colDef.ddlVal()
}
