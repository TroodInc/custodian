package object

import (
	"bytes"
	"custodian/logger"
	"custodian/server/object/description"
	"encoding/json"
	"fmt"
	"github.com/Q-CIS-DEV/go-rql-parser"
	"strconv"
	"strings"
	"text/template"
)

//https://doc.apsstandard.org/2.1/spec/rql/
//It's important that using the join with distinct or group by is more worse then exists, see exmaple: https://danmartensen.svbtle.com/sql-performance-of-join-and-where-exists

const (
	ErrRQLInternal         = "internal"
	ErrRQLWrong            = "wrong"
	ErrRQLUnknownOperator  = "unknown_operator"
	ErrRQLUnknownValueFunc = "unknown_value_function"
	ErrRQLWrongFieldName   = "wrong_field_name"
	ErrRQLWrongValue       = "wrong_value"
)

type RqlError struct {
	code string
	msg  string
}

func (e *RqlError) Error() string {
	return fmt.Sprintf("RQL error: code='%s'  msg = '%s'", e.code, e.msg)
}

func (e *RqlError) Json() []byte {
	j, _ := json.Marshal(map[string]string{
		"code": "rql:" + e.code,
		"msg":  e.msg,
	})
	return j
}

func NewRqlError(code string, msg string, a ...interface{}) *RqlError {
	return &RqlError{code: code, msg: fmt.Sprintf(msg, a...)}
}

type SqlQuery struct {
	Where  string
	Binds  []interface{}
	Sort   string
	Limit  string
	Offset string
}

type SqlTranslator struct {
	rootNode *rqlParser.RqlRootNode
}

func NewSqlTranslator(rqlRoot *rqlParser.RqlRootNode) *SqlTranslator {
	return &SqlTranslator{rootNode: rqlRoot}
}

type context struct {
	root     *Node
	tblAlias string
	binds    []interface{}
}

func (ctx *context) addBind(v interface{}) string {
	ctx.binds = append(ctx.binds, v)
	b := bytes.NewBufferString("$")
	b.WriteString(strconv.Itoa(len(ctx.binds)))
	return b.String()
}

type Exists struct {
	Table            string
	Alias            string
	FK               string
	RightTableAlias  string
	RightTableColumn string
	GenericTypeField string
	GenericType      string
}

const templExists = `SELECT 1 FROM {{.Table}} {{.Alias}} WHERE {{.Alias}}.{{.FK}}={{.RightTableAlias}}.{{.RightTableColumn}}{{if .GenericTypeField}}::text{{end}}{{if .GenericTypeField }} AND {{.Alias}}.{{.GenericTypeField}}='{{.GenericType}}' {{end}}`

var parsedTemplExists = template.Must(template.New("dml_rql_exists").Parse(templExists))

type expr func() string
type operator func(*context, []interface{}) (expr, error)
type valueFunc func([]interface{}) (interface{}, error)

var operators map[string]operator = make(map[string]operator)
var valueFuncs map[string]valueFunc = make(map[string]valueFunc)

func init() {
	operators["AND"] = and
	operators["OR"] = or
	operators["NOT"] = not
	operators["EQ"] = eq
	operators["NE"] = ne
	operators["IN"] = in
	operators["LT"] = lt
	operators["LE"] = le
	operators["GT"] = gt
	operators["GE"] = ge
	operators["LIKE"] = like
	operators["IS_NULL"] = is_null

	valueFuncs["NULL"] = nullvf
	valueFuncs["EMPTY"] = emptyvf
	valueFuncs["TRUE"] = truevf
	valueFuncs["FALSE"] = falsevf
}

func (ctx *context) nodeToOpExpr(node *rqlParser.RqlNode) (expr, error) {
	operator, ok := operators[strings.ToUpper(node.Op)]
	if !ok {
		return nil, NewRqlError(ErrRQLUnknownOperator, "RQL operator '%s' is unknown", node.Op)
	}
	//TODO: move translation logic into another structure, goRqlParser.SqlTranslator seems to be suitable
	if strings.ToUpper(node.Op) == "LIKE" {
		//	need replace * with %
		argumentRunes := []rune( node.Args[1].(string))
		if argumentRunes[0] == '*' {
			argumentRunes[0] = '%'
		}
		if argumentRunes[len(argumentRunes)-1] == '*' {
			argumentRunes[len(argumentRunes)-1] = '%'
		}
		node.Args[1] = string(argumentRunes)
	}
	return operator(ctx, node.Args)
}

func (ctx *context) argToOpExpr(arg interface{}) (expr, error) {
	node, ok := arg.(*rqlParser.RqlNode)
	if !ok {
		logger.Error("Can't convert argument '%s' to expresion ", arg)
		return nil, NewRqlError(ErrRQLWrong, "Unexpected argument: %s", arg)
	}
	return ctx.nodeToOpExpr(node)
}

func (ctx *context) argsToOpExpr(args []interface{}, sep string) (expr, error) {
	exprs := make([]expr, len(args), len(args))
	for i := range args {
		e, err := ctx.argToOpExpr(args[i])
		if err != nil {
			return nil, err
		}
		exprs[i] = e
	}

	return func() string {
		b := bytes.NewBufferString("(")
		for i := range exprs {
			b.WriteString(exprs[i]())
			b.WriteString(sep)
		}
		b.Truncate(b.Len() - len(sep))
		b.WriteRune(')')
		return b.String()
	}, nil
}

func argToFieldVal(arg interface{}, field *FieldDescription) (interface{}, error) {
	switch value := arg.(type) {
	case *rqlParser.RqlNode:
		vf, ok := valueFuncs[strings.ToUpper(value.Op)]
		if !ok {
			return nil, NewRqlError(ErrRQLUnknownValueFunc, "Value function '%s' is unknown", value.Op)
		}
		val, err := vf(value.Args)
		if err != nil {
			return nil, err
		}
		if val != nil && !field.IsValueTypeValid(val) {
			t, _ := field.Type.String()
			return nil, NewRqlError(ErrRQLWrongValue, "Value '%s' is wrong. Expected: %s", value, t)
		}
		return val, nil
	case string:
		return field.ValueFromString(value)
	default:
		return nil, NewRqlError(ErrRQLWrongValue, "Unknown operator's value type: '%s'", value)
	}
}

/* value functions */
func nullvf(args []interface{}) (interface{}, error) {
	return nil, nil
}

func emptyvf(args []interface{}) (interface{}, error) {
	return "", nil
}

func truevf(args []interface{}) (interface{}, error) {
	return true, nil
}

func falsevf(args []interface{}) (interface{}, error) {
	return false, nil
}

/* operators */
func and(ctx *context, args []interface{}) (expr, error) {
	return ctx.argsToOpExpr(args, " AND ")
}

func or(ctx *context, args []interface{}) (expr, error) {
	return ctx.argsToOpExpr(args, " OR ")
}

func not(ctx *context, args []interface{}) (expr, error) {
	if len(args) != 1 {
		return nil, NewRqlError(ErrRQLWrong, "Expected only one argument for '%s' rql function but founded '%d'", "not", len(args))
	}
	notExpr, err := ctx.argToOpExpr(args[0])
	if err != nil {
		return nil, err
	}
	return func() string {
		return "NOT (" + notExpr() + ")"
	}, nil
}

type sqlOp func(*FieldDescription, []interface{}) (string, error)

//Assemble SQL for the given expression
func (ctx *context) makeFieldExpression(args []interface{}, sqlOperator sqlOp) (expr, error) {
	// 	Recursively walk through each object building joins between tables and query by value at the end
	//	example: handling "eq(fruit_tags.tag_id,1)" the function will make 1 join with "fruit_tags" and 1 filter with
	//	"tag_id=1"
	fieldPath, ok := args[0].(string)
	if !ok {
		return nil, NewRqlError(ErrRQLWrongFieldName, "The field name is not string")
	}

	var expression bytes.Buffer
	alias := ctx.tblAlias

	currentNode := ctx.root.Clone()
	currentMeta := currentNode.Meta
	joinsCount := 0

	//split fieldPath into separate fieldPathParts
	fieldPathParts := strings.Split(fieldPath, ".")

	for i := 0; i < len(fieldPathParts); i++ {
		//dynamically build child nodes for Query mode

		field := currentMeta.FindField(fieldPathParts[i])

		if field == nil {
			return nil, NewRqlError(ErrRQLWrongFieldName, "Object '%s' doesn't have '%s' field", currentMeta.Name, fieldPathParts[i])
		}
		// process related object`s table join
		// do it only if the current iteration is not that last, because the target field for the query can have the
		// "LinkMeta"
		if field.Type == description.FieldTypeGeneric {
			if field.LinkType == description.LinkTypeInner {
				//apply filtering by generic fields _object value and skip the next iteration
				i++
				if fieldPathParts[i] != GenericInnerLinkObjectKey {
					expression.WriteString(alias)
					expression.WriteRune('.')
					expression.WriteString(GetGenericFieldTypeColumnName(field.Name))
					expression.WriteString(fmt.Sprintf("='%s'", fieldPathParts[i]))
					expression.WriteString(" AND ")
				}
			}
		} else if field.Type == description.FieldTypeObjects {
			//query which uses "Objects" field has to be replaced with query using LinkThrough
			//eg: object A has "Objects" relation "bs" which references B. Query like eq(bs.name,Fedor) will be replace
			//with query eq(a__b_set.b.name,Fedor) and processed in the common way
			outerField := field.LinkThrough.FindField(field.Meta.Name).ReverseOuterField().Name
			throughObjectFieldName := field.LinkMeta.Name
			fieldPathParts = append(fieldPathParts[:i], append([]string{outerField, throughObjectFieldName}, fieldPathParts[i+1:]...)...)
			i--
			continue
		}

		//fieldPathParts[len(fieldPathParts)-1] != types.GenericInnerLinkObjectKey
		if i != len(fieldPathParts)-1 {
			currentNode.RecursivelyFillChildNodes(currentNode.Depth+1, description.FieldModeQuery)

			linkedMeta := ctx.getMetaToJoin(field, fieldPathParts[i:])
			if linkedMeta != nil {
				joinsCount++
				//fill in all the options required for join operation
				exists := &Exists{Table: GetTableName(linkedMeta.Name), Alias: alias + field.Name, RightTableAlias: alias}
				if field.OuterLinkField != nil {
					exists.RightTableColumn = linkedMeta.Key.Name
					if field.Type == description.FieldTypeGeneric {
						exists.FK = GetGenericFieldKeyColumnName(field.OuterLinkField.Name)
						exists.GenericTypeField = GetGenericFieldTypeColumnName(field.OuterLinkField.Name)
						exists.GenericType = currentMeta.Name
					} else {
						exists.FK = field.OuterLinkField.Name
					}
				} else {
					if field.Type == description.FieldTypeGeneric {
						//cast object PK to string, because join is performed by generic __id field, which has string type
						exists.FK = linkedMeta.Key.Name + "::text"
						exists.RightTableColumn = GetGenericFieldKeyColumnName(field.Name)
					} else {
						exists.RightTableColumn = field.Name
						exists.FK = linkedMeta.Key.Name
					}
				}

				expression.WriteString("EXISTS (")
				if err := parsedTemplExists.Execute(&expression, exists); err != nil {
					logger.Error("RQL 'exists' template processing error: %s", err.Error())
					return nil, NewRqlError(ErrRQLInternal, err.Error())
				}

				expectedNode, ok := currentNode.ChildNodes.Get(field.Name)
				if field.Type == description.FieldTypeGeneric {
					if field.LinkType == description.LinkTypeInner {
						expectedNode = &Node{
							KeyField:     linkedMeta.Key,
							Meta:         linkedMeta,
							ChildNodes:   *NewChildNodes(),
							Depth:        1,
							OnlyLink:     false,
							Parent:       nil,
							Type:         NodeTypeRegular,
							SelectFields: *NewSelectFields(linkedMeta.Key, linkedMeta.TableFields()),
						}
					}
				}
				if !ok {
					return nil, NewRqlError(ErrRQLWrongFieldName, "Object '%s' doesn't have '%s' branch", currentNode.Meta.Name, field.Name)
				}
				currentNode = expectedNode

				currentMeta = linkedMeta
				alias = exists.Alias
				expression.WriteString(" AND ")
			} else {
				return nil, NewRqlError(ErrRQLWrongFieldName, "FieldDescription path '%s' in 'eq' rql function is incorrect", fieldPath)
			}
		} else {
			var fieldName string
			if field.Type == description.FieldTypeGeneric {
				fieldName = GetGenericFieldTypeColumnName(field.Name)
			} else {
				fieldName = field.Name
			}

			op, err := sqlOperator(field, args[1:])
			if err != nil {
				return nil, err
			}

			expression.WriteString(fmt.Sprintf("%s.\"%s\" %s", alias, fieldName, op))
		}
	}

	//close expressions
	for i := 0; i < joinsCount; i++ {
		expression.WriteRune(')')
	}
	return func() string {
		return expression.String()
	}, nil
}

//get meta to join based on fieldDescription and query path
//eg: queryPath is "target.a.name" and field is generic, then A meta should be returned
//regular field case is straightforward
func (ctx *context) getMetaToJoin(fieldDescription *FieldDescription, queryPath []string) *Meta {
	if fieldDescription.Type == description.FieldTypeGeneric && fieldDescription.LinkType == description.LinkTypeInner {
		return fieldDescription.LinkMetaList.GetByName(queryPath[0])
	} else {
		return fieldDescription.LinkMeta
	}
}

func (ctx *context) sqlOpIN(field *FieldDescription, args []interface{}) (string, error) {
	expression := bytes.NewBufferString("IN (")
	if valuesNode, ok := args[0].(*rqlParser.RqlNode); ok {
		//case of list of values
		for i := range valuesNode.Args {
			value, err := argToFieldVal(valuesNode.Args[i], field)
			if err != nil {
				return "", err
			}
			expression.WriteString(ctx.addBind(value))
			if i < len(valuesNode.Args)-1 {
				expression.WriteRune(',')
			}
		}
	} else {
		//case of single value
		value, err := argToFieldVal(args[0].(string), field)
		if err != nil {
			return "", err
		}
		expression.WriteString(ctx.addBind(value))
	}
	expression.WriteString(")")
	return expression.String(), nil
}

func (ctx *context) sqlOpSimple(op string) sqlOp {
	return func(f *FieldDescription, vals []interface{}) (string, error) {
		v, err := argToFieldVal(vals[0], f)
		if err != nil {
			return "", err
		}

		if v == nil {
			return "", NewRqlError(ErrRQLWrongValue, "Operators '%s' doesn't support NULL value", op)
		}

		p := bytes.NewBufferString(op)
		p.WriteString(ctx.addBind(v))
		return p.String(), nil
	}
}

func in(ctx *context, args []interface{}) (expr, error) {
	if len(args) == 1 {
		args = append(args, "")
	}
	if len(args) < 2 {
		return nil, NewRqlError(ErrRQLWrong, "Expected exactly one argument for '%s' rql function but found '%d'", "in", len(args))
	}
	return ctx.makeFieldExpression(args, ctx.sqlOpIN)
}
func eq(ctx *context, args []interface{}) (expr, error) {
	if len(args) != 2 {
		return nil, NewRqlError(ErrRQLWrong, "Expected only two arguments for '%s' rql function but founded '%d'", "eq", len(args))
	}
	return ctx.makeFieldExpression(args, ctx.sqlOpSimple("="))
}
func ne(ctx *context, args []interface{}) (expr, error) {
	if len(args) != 2 {
		return nil, NewRqlError(ErrRQLWrong, "Expected only two arguments for '%s' rql function but founded '%d'", "ne", len(args))
	}
	return ctx.makeFieldExpression(args, ctx.sqlOpSimple("!="))
}
func lt(ctx *context, args []interface{}) (expr, error) {
	if len(args) != 2 {
		return nil, NewRqlError(ErrRQLWrong, "Expected only two arguments for '%s' rql function but founded '%d'", "lt", len(args))
	}
	return ctx.makeFieldExpression(args, ctx.sqlOpSimple("<"))
}
func le(ctx *context, args []interface{}) (expr, error) {
	if len(args) != 2 {
		return nil, NewRqlError(ErrRQLWrong, "Expected only two arguments for '%s' rql function but founded '%d'", "le", len(args))
	}
	return ctx.makeFieldExpression(args, ctx.sqlOpSimple("<="))
}
func gt(ctx *context, args []interface{}) (expr, error) {
	if len(args) != 2 {
		return nil, NewRqlError(ErrRQLWrong, "Expected only two arguments for '%s' rql function but founded '%d'", "gt", len(args))
	}
	return ctx.makeFieldExpression(args, ctx.sqlOpSimple(">"))
}
func ge(ctx *context, args []interface{}) (expr, error) {
	if len(args) != 2 {
		return nil, NewRqlError(ErrRQLWrong, "Expected only two arguments for '%s' rql function but founded '%d'", "ge", len(args))
	}
	return ctx.makeFieldExpression(args, ctx.sqlOpSimple(">="))
}
func like(ctx *context, args []interface{}) (expr, error) {
	if len(args) != 2 {
		return nil, NewRqlError(ErrRQLWrong, "Expected only two arguments for '%s' rql function but founded '%d'", "like", len(args))
	}
	return ctx.makeFieldExpression(args, ctx.sqlOpSimple("ILIKE "))
}

func is_null(ctx *context, args []interface{}) (expr, error) {
	if len(args) != 2 {
		return nil, NewRqlError(ErrRQLWrong, "Expected only two arguments for '%s' rql function but founded '%d'", "is_null", len(args))
	}

	var res = "IS NULL"

	value, err := strconv.ParseBool(args[1].(string))

	if err != nil {
		return nil, NewRqlError(ErrRQLWrong, "Second argument for is_null() must be 'true' or 'false'")
	} else if value == false {
		 res = "IS NOT NULL"
	}

	return ctx.makeFieldExpression(args, func(f *FieldDescription, vals []interface{}) (string, error) {
		return res, nil
	})
}

func (st *SqlTranslator) sort(tableAlias string, root *Node) (string, error) {
	var b bytes.Buffer
	var sorts = make([]rqlParser.Sort, 0)

	if st.rootNode.Sort() != nil {
		sorts = st.rootNode.Sort()
	}

	sorts = append(sorts, rqlParser.Sort{By: root.Meta.MetaDescription.Key, Desc: false})
	for i := range sorts {
		fieldDescription := root.Meta.FindField(sorts[i].By)
		if fieldDescription == nil {
			return "", NewRqlError(ErrRQLWrongFieldName, "Object '%s' doesn't have '%s' field", root.Meta.Name, sorts[i].By)
		}
		b.WriteString(tableAlias)
		b.WriteRune('.')
		b.WriteString(sorts[i].By)
		if sorts[i].Desc {
			b.WriteString(" DESC")
		}
		b.WriteRune(',')
	}

	if b.Len() > 0 {
		b.Truncate(b.Len() - 1)
	}

	return b.String(), nil
}

func (st *SqlTranslator) query(tableAlias string, root *Node) (*SqlQuery, error) {
	ctx := &context{root: root, tblAlias: tableAlias, binds: make([]interface{}, 0)}
	var whereStatement string
	if st.rootNode.Node != nil {
		whereExp, err := ctx.nodeToOpExpr(st.rootNode.Node)
		if err != nil {
			return nil, err
		}
		whereStatement = whereExp()
	} else {
		whereStatement = ""
	}

	sort, err := st.sort(tableAlias, root)
	if err != nil {
		return nil, err
	}

	return &SqlQuery{Where: whereStatement, Binds: ctx.binds, Sort: sort, Limit: st.rootNode.Limit(), Offset: st.rootNode.Offset()}, nil
}
