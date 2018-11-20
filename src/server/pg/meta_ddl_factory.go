package pg

import (
	"server/object/meta"
	"fmt"
	"server/object/description"
)

type MetaDdlFactory struct{}

func (metaDdlFactory *MetaDdlFactory) Factory(m *meta.Meta) (*MetaDDL, error) {
	var metaDdl = &MetaDDL{Table: GetTableName(m.Name), Pk: m.Key.Name}
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
	} else if field.Type == description.FieldTypeObject && field.LinkType == description.LinkTypeInner {
		return metaDdlFactory.processInnerLinkField(field)
	} else if field.LinkType == description.LinkTypeOuter {
		return metaDdlFactory.processOuterLinkField(field)
	} else if field.Type == description.FieldTypeGeneric && field.LinkType == description.LinkTypeInner {
		return metaDdlFactory.processGenericInnerLinkField(field)
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

	ifk := IFK{
		FromColumn: field.Name,
		ToTable:    GetTableName(field.LinkMeta.Name),
		ToColumn:   field.LinkMeta.Key.Name,
		OnDelete:   field.OnDelete.ToDbValue(),
		Default:    column.Defval,
	}

	return []Column{*column}, &ifk, nil, metaDdlFactory.factorySequence(field), nil
}

func (metaDdlFactory *MetaDdlFactory) processOuterLinkField(field *meta.FieldDescription) ([]Column, *IFK, *OFK, *Seq, error) {
	outerForeignKey := OFK{FromTable: GetTableName(field.LinkMeta.Name), FromColumn: field.OuterLinkField.Name, ToTable: GetTableName(field.Meta.Name), ToColumn: field.Meta.Key.Name}
	return nil, nil, &outerForeignKey, nil, nil
}

func (metaDdlFactory *MetaDdlFactory) processGenericInnerLinkField(field *meta.FieldDescription) ([]Column, *IFK, *OFK, *Seq, error) {
	typeColumn := Column{}
	typeColumn.Name = meta.GetGenericFieldTypeColumnName(field.Name)
	typeColumn.Typ = ColumnTypeText
	typeColumn.Optional = field.Optional
	typeColumn.Unique = false

	keyColumn := Column{}
	keyColumn.Name = meta.GetGenericFieldKeyColumnName(field.Name)
	keyColumn.Typ = ColumnTypeText
	keyColumn.Optional = field.Optional
	keyColumn.Unique = false

	return []Column{typeColumn, keyColumn}, nil, nil, metaDdlFactory.factorySequence(field), nil
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
