package pg

import (
	"fmt"
	"server/object/meta"
)

type MetaDdlFactory struct {
}

func (mdf *MetaDdlFactory) Factory(metaDescription *meta.Meta) (*MetaDDL, error) {
	var metaDdl = &MetaDDL{Table: GetTableName(metaDescription.Name), Pk: metaDescription.Key}
	metaDdl.Columns = make([]Column, 0, )
	metaDdl.IFKs = make([]IFK, 0)
	metaDdl.OFKs = make([]OFK, 0)
	metaDdl.Seqs = make([]Seq, 0)
	for _, field := range metaDescription.Fields {
		if columns, ifk, ofk, seq, err := mdf.FactoryFieldProperties(field, metaDescription.Name, metaDescription.Key); err != nil {
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

func (mdf *MetaDdlFactory) FactoryFieldProperties(field *meta.Field, metaName string, metaKey string) ([]Column, *IFK, *OFK, *Seq, error) {
	if field.IsSimple() {
		return mdf.factorySimpleFieldProperties(field, metaName)
	} else if field.Type == meta.FieldTypeObject && field.LinkType == meta.LinkTypeInner {
		return mdf.processInnerLinkField(field, metaName)
	} else if field.LinkType == meta.LinkTypeOuter && field.Type == meta.FieldTypeArray {
		return mdf.processOuterLinkField(field, metaName, metaKey)
	} else if field.Type == meta.FieldTypeObjects {
		return mdf.processObjectsInnerLinkField(field, metaName, metaKey)
	} else if field.Type == meta.FieldTypeGeneric && field.LinkType == meta.LinkTypeInner {
		return mdf.processGenericInnerLinkField(metaName, field)
	} else if field.Type == meta.FieldTypeGeneric && field.LinkType == meta.LinkTypeOuter {
		return nil, nil, nil, nil, nil
	} else {
		return nil, nil, nil, nil, &DDLError{table: metaName, code: ErrUnsupportedLinkType, msg: fmt.Sprintf("Unsupported link type lt = %v, ft = %v", string(field.LinkType), string(field.LinkType))}

	}

	return nil, nil, nil, nil, nil
}

func (mdf *MetaDdlFactory) factorySimpleFieldProperties(field *meta.Field, metaName string) ([]Column, *IFK, *OFK, *Seq, error) {
	column, err := mdf.factoryBlankColumn(metaName, field)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return []Column{*column}, nil, nil, mdf.factorySequence(metaName, field), nil
}

func (mdf *MetaDdlFactory) processInnerLinkField(field *meta.Field, metaName string) ([]Column, *IFK, *OFK, *Seq, error) {
	column, err := mdf.factoryBlankColumn(metaName, field)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	column.Typ = field.LinkMeta.FindField(field.LinkMeta.Key).Type

	ifk := IFK{
		FromColumn: field.Name,
		ToTable:    GetTableName(field.LinkMeta.Name),
		ToColumn:   field.LinkMeta.Key,
		OnDelete:   field.OnDeleteStrategy().ToDbValue(),
		Default:    column.Defval,
	}

	return []Column{*column}, &ifk, nil, mdf.factorySequence(metaName, field), nil
}

func (mdf *MetaDdlFactory) processObjectsInnerLinkField(field *meta.Field, metaName string, metaKey string) ([]Column, *IFK, *OFK, *Seq, error) {
	outerForeignKey := OFK{FromTable: GetTableName(field.LinkThrough.Name), FromColumn: field.OuterLinkField.Name, ToTable: GetTableName(metaName), ToColumn: metaKey}
	return nil, nil, &outerForeignKey, nil, nil
}

func (mdf *MetaDdlFactory) processOuterLinkField(field *meta.Field, metaName string, metaKey string) ([]Column, *IFK, *OFK, *Seq, error) {
	outerForeignKey := OFK{FromTable: GetTableName(field.LinkMeta.Name), FromColumn: field.OuterLinkField.Name, ToTable: GetTableName(metaName), ToColumn: metaKey}
	return nil, nil, &outerForeignKey, nil, nil
}

func (mdf *MetaDdlFactory) processGenericInnerLinkField(metaName string, field *meta.Field) ([]Column, *IFK, *OFK, *Seq, error) {
	typeColumn := Column{}
	typeColumn.Name = meta.GetGenericFieldTypeColumnName(field.Name)
	typeColumn.Typ = meta.FieldTypeString
	typeColumn.Optional = field.Optional
	typeColumn.Unique = false

	keyColumn := Column{}
	keyColumn.Name = meta.GetGenericFieldKeyColumnName(field.Name)
	keyColumn.Typ = meta.FieldTypeString
	keyColumn.Optional = field.Optional
	keyColumn.Unique = false

	return []Column{typeColumn, keyColumn}, nil, nil, mdf.factorySequence(metaName, field), nil
}

// factory common column
func (mdf *MetaDdlFactory) factoryBlankColumn(metaName string, field *meta.Field) (*Column, error) {
	column := Column{}
	column.Name = field.Name
	column.Typ = field.Type
	column.Optional = field.Optional
	column.Unique = field.Unique

	var err error
	column.Defval, err = mdf.getDefaultValue(metaName, field)
	if column.Defval, err = mdf.getDefaultValue(metaName, field); err != nil {
		return nil, err
	}

	return &column, nil
}

// factory default value sequence
func (mdf *MetaDdlFactory) factorySequence(metaName string, field *meta.Field) *Seq {
	colDef, err := newColDefVal(metaName, field)
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
func (mdf *MetaDdlFactory) getDefaultValue(metaName string, field *meta.Field) (string, error) {
	colDef, err := newColDefVal(metaName, field)
	if err != nil {
		return "", err
	}
	return colDef.ddlVal()
}

func NewMetaDdlFactory() *MetaDdlFactory {
	return &MetaDdlFactory{}
}
