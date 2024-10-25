package object

import (
	"custodian/server/object/description"
	"fmt"
)

type MetaDdlFactory struct {
	metaDescriptionSyncer MetaDescriptionSyncer
}

func (mdf *MetaDdlFactory) Factory(metaDescription *description.MetaDescription) (*MetaDDL, error) {
	var metaDdl = &MetaDDL{Table: GetTableName(metaDescription.Name), Pk: metaDescription.Key}
	metaDdl.Columns = make([]Column, 0, )
	metaDdl.IFKs = make([]IFK, 0)
	metaDdl.OFKs = make([]OFK, 0)
	metaDdl.Seqs = make([]Seq, 0)
	for _, field := range metaDescription.Fields {
		if columns, ifk, ofk, seq, err := mdf.FactoryFieldProperties(&field, metaDescription); err != nil {
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

func (mdf *MetaDdlFactory) FactoryFieldProperties(field *description.Field, metaDescription *description.MetaDescription) ([]Column, *IFK, *OFK, *Seq, error) {
	if field.IsSimple() {
		return mdf.factorySimpleFieldProperties(field, metaDescription.Name)
	} else if field.Type == description.FieldTypeObject && field.LinkType == description.LinkTypeInner {
		return mdf.processInnerLinkField(field, metaDescription)
	} else if field.LinkType == description.LinkTypeOuter && field.Type == description.FieldTypeArray {
		return mdf.processOuterLinkField(field, metaDescription)
	} else if field.Type == description.FieldTypeObjects {
		return mdf.processObjectsInnerLinkField(field, metaDescription)
	} else if field.Type == description.FieldTypeGeneric && field.LinkType == description.LinkTypeInner {
		return mdf.processGenericInnerLinkField(metaDescription.Name, field)
	} else if field.Type == description.FieldTypeGeneric && field.LinkType == description.LinkTypeOuter {
		return nil, nil, nil, nil, nil
	} else {
		return nil, nil, nil, nil, &DDLError{table: metaDescription.Name, code: ErrUnsupportedLinkType, msg: fmt.Sprintf("Unsupported link type lt = %v, ft = %v", string(rune(field.LinkType)), string(rune(field.LinkType)))}

	}
}

func (mdf *MetaDdlFactory) factorySimpleFieldProperties(field *description.Field, metaName string) ([]Column, *IFK, *OFK, *Seq, error) {
	column, err := mdf.factoryBlankColumn(metaName, field)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return []Column{*column}, nil, nil, mdf.factorySequence(metaName, field), nil
}

func (mdf *MetaDdlFactory) processInnerLinkField(field *description.Field, metaDescription *description.MetaDescription) ([]Column, *IFK, *OFK, *Seq, error) {
	column, err := mdf.factoryBlankColumn(metaDescription.Name, field)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	linkMetaDescription, _, err := mdf.metaDescriptionSyncer.Get(field.LinkMeta)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	column.Typ = linkMetaDescription.FindField(linkMetaDescription.Key).Type

	ifk := IFK{
		FromColumn: field.Name,
		ToTable:    GetTableName(linkMetaDescription.Name),
		ToColumn:   linkMetaDescription.Key,
		OnDelete:   field.OnDeleteStrategy().ToDbValue(),
		Default:    column.Defval,
	}

	return []Column{*column}, &ifk, nil, mdf.factorySequence(metaDescription.Name, field), nil
}

func (mdf *MetaDdlFactory) processObjectsInnerLinkField(field *description.Field, metaDescription *description.MetaDescription) ([]Column, *IFK, *OFK, *Seq, error) {
	outerForeignKey := OFK{FromTable: GetTableName(field.LinkThrough), FromColumn: field.OuterLinkField, ToTable: GetTableName(metaDescription.Name), ToColumn: metaDescription.Key}
	return nil, nil, &outerForeignKey, nil, nil
}

func (mdf *MetaDdlFactory) processOuterLinkField(field *description.Field, metaDescription *description.MetaDescription) ([]Column, *IFK, *OFK, *Seq, error) {
	outerForeignKey := OFK{FromTable: GetTableName(field.LinkMeta), FromColumn: field.OuterLinkField, ToTable: GetTableName(metaDescription.Name), ToColumn: metaDescription.Key}
	return nil, nil, &outerForeignKey, nil, nil
}

func (mdf *MetaDdlFactory) processGenericInnerLinkField(metaName string, field *description.Field) ([]Column, *IFK, *OFK, *Seq, error) {
	typeColumn := Column{}
	typeColumn.Name = GetGenericFieldTypeColumnName(field.Name)
	typeColumn.Typ = description.FieldTypeString
	typeColumn.Optional = field.Optional
	typeColumn.Unique = false

	keyColumn := Column{}
	keyColumn.Name = GetGenericFieldKeyColumnName(field.Name)
	keyColumn.Typ = description.FieldTypeString
	keyColumn.Optional = field.Optional
	keyColumn.Unique = false

	return []Column{typeColumn, keyColumn}, nil, nil, mdf.factorySequence(metaName, field), nil
}

// factory common column
func (mdf *MetaDdlFactory) factoryBlankColumn(metaName string, field *description.Field) (*Column, error) {
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
	if len(field.Enum) > 0 {
		column.Enum = field.Enum
	}

	return &column, nil
}

// factory default value sequence
func (mdf *MetaDdlFactory) factorySequence(metaName string, field *description.Field) *Seq {
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
func (mdf *MetaDdlFactory) getDefaultValue(metaName string, field *description.Field) (string, error) {
	colDef, err := newColDefVal(metaName, field)
	if err != nil {
		return "", err
	}
	return colDef.ddlVal()
}

func NewMetaDdlFactory(metaDescriptionSyncer MetaDescriptionSyncer) *MetaDdlFactory {
	return &MetaDdlFactory{metaDescriptionSyncer: metaDescriptionSyncer}
}
