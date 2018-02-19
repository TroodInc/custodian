package pg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"logger"
	"server/data"
	"server/meta"
	rqlParser "github.com/reaxoft/go-rql-parser"
	"net/url"
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
	root     *data.Node
	tblAlias string
	binds    []interface{}
}

func (ctx *context) addBind(v interface{}) string {
	ctx.binds = append(ctx.binds, v)
	b := bytes.NewBufferString("$")
	b.WriteString(strconv.Itoa(len(ctx.binds)))
	return b.String()
}

type exists struct {
	Table  string
	Alias  string
	FK     string
	RAlias string
	RCol   string
}

const (
	templExists = `SELECT 1 FROM {{.Table}} {{.Alias}} WHERE {{.Alias}}.{{.FK}}={{.RAlias}}.{{.RCol}}`
)

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

func argToField(arg interface{}) (string, error) {
	field, ok := arg.(string)
	if !ok {
		return "", NewRqlError(ErrRQLWrongFieldName, "The field name is not string")
	}
	return field, nil
}

func argToFieldVal(arg interface{}, f *meta.Field) (interface{}, error) {
	switch v := arg.(type) {
	case *rqlParser.RqlNode:
		vf, ok := valueFuncs[strings.ToUpper(v.Op)]
		if !ok {
			return nil, NewRqlError(ErrRQLUnknownValueFunc, "Value function '%s' is unknown", v.Op)
		}
		val, err := vf(v.Args)
		if err != nil {
			return nil, err
		}
		if val != nil && !f.IsValueTypeValid(val) {
			t, _ := f.Type.String()
			return nil, NewRqlError(ErrRQLWrongValue, "Value '%s' is wrong. Expected: %s", v, t)
		}
		return val, nil
	case string:
		unescaped, err := url.QueryUnescape(v)
		if err != nil {
			return nil, NewRqlError(ErrRQLWrongValue, "Can't unescape '%s' value: %s", arg, err.Error())
		}
		return f.ValueFromString(unescaped)
	default:
		return nil, NewRqlError(ErrRQLWrongValue, "Unknown operator's value type: '%s'", v)
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

type sqlOp func(*meta.Field, []interface{}) (string, error)

func (ctx *context) fieldExpr(args []interface{}, sqlOperator sqlOp) (expr, error) {
	fieldPath, ok := args[0].(string)
	if !ok {
		return nil, NewRqlError(ErrRQLWrongFieldName, "The field name is not string")
	}

	var p bytes.Buffer
	alias := ctx.tblAlias
	node := ctx.root
	fields := strings.Split(fieldPath, ".")
	for i, fieldName := range fields {
		f := node.Meta.FindField(fieldName)
		if f == nil {
			return nil, NewRqlError(ErrRQLWrongFieldName, "Object '%s' doesn't have '%s' field", node.Meta.Name, fieldName)
		}

		if f.LinkMeta != nil {
			exst := &exists{Table: tblName(f.LinkMeta), Alias: alias + fieldName, RAlias: alias}
			if f.OuterLinkField != nil {
				exst.FK = f.OuterLinkField.Name
				exst.RCol = f.Meta.Key.Name
			} else {
				exst.FK = f.LinkMeta.Key.Name
				exst.RCol = f.Name
			}

			p.WriteString("EXISTS (")
			if err := parsedTemplExists.Execute(&p, exst); err != nil {
				logger.Error("RQL 'exists' template processing error: %s", err.Error())
				return nil, NewRqlError(ErrRQLInternal, err.Error())
			}
			p.WriteString(" AND ")

			n, ok := node.Branches[fieldName]
			if !ok {
				return nil, NewRqlError(ErrRQLWrongFieldName, "Object '%s' doesn't have '%s' branch", node.Meta.Name, fieldName)
			}
			node = n
			alias = exst.Alias
		} else {
			if i != len(fields)-1 {
				return nil, NewRqlError(ErrRQLWrongFieldName, "Field path '%s' in 'eq' rql function is incorrect", fieldPath)
			}

			p.WriteString(alias)
			p.WriteRune('.')
			p.WriteString(fieldName)
			p.WriteRune(' ')
			op, err := sqlOperator(f, args[1:])
			if err != nil {
				return nil, err
			}
			p.WriteString(op)
		}
	}

	for i := 0; i < len(fields)-1; i++ {
		p.WriteRune(')')
	}
	return func() string {
		return p.String()
	}, nil
}

func (ctx *context) sqlOpIN(f *meta.Field, vals []interface{}) (string, error) {
	p := bytes.NewBufferString("IN (")
	for i := range vals {
		v, err := argToFieldVal(vals[i], f)
		if err != nil {
			return "", err
		}
		p.WriteString(ctx.addBind(v))
		p.WriteRune(',')
	}
	p.Truncate(p.Len() - 1)
	p.WriteString(")")
	return p.String(), nil
}

func (ctx *context) sqlOpEQ(f *meta.Field, vals []interface{}) (string, error) {
	v, err := argToFieldVal(vals[0], f)
	if err != nil {
		return "", err
	}

	var p bytes.Buffer
	if v == nil {
		//special for null processing
		p.WriteString(" IS NULL")
	} else {
		p.WriteString("=")
		p.WriteString(ctx.addBind(v))
	}
	return p.String(), nil
}

func (ctx *context) sqlOpNE(f *meta.Field, vals []interface{}) (string, error) {
	v, err := argToFieldVal(vals[0], f)
	if err != nil {
		return "", err
	}

	var p bytes.Buffer
	if v == nil {
		//special for null processing
		p.WriteString(" IS NOT NULL")
	} else {
		p.WriteString("!=")
		p.WriteString(ctx.addBind(v))
	}
	return p.String(), nil
}

func (ctx *context) sqlOpSimple(op string) sqlOp {
	return func(f *meta.Field, vals []interface{}) (string, error) {
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
	if len(args) < 2 {
		return nil, NewRqlError(ErrRQLWrong, "Expected more then one argument for '%s' rql function but founded '%d'", "in", len(args))
	}
	return ctx.fieldExpr(args, ctx.sqlOpIN)
}
func eq(ctx *context, args []interface{}) (expr, error) {
	if len(args) != 2 {
		return nil, NewRqlError(ErrRQLWrong, "Expected only two arguments for '%s' rql function but founded '%d'", "eq", len(args))
	}
	return ctx.fieldExpr(args, ctx.sqlOpEQ)
}
func ne(ctx *context, args []interface{}) (expr, error) {
	if len(args) != 2 {
		return nil, NewRqlError(ErrRQLWrong, "Expected only two arguments for '%s' rql function but founded '%d'", "ne", len(args))
	}
	return ctx.fieldExpr(args, ctx.sqlOpNE)
}
func lt(ctx *context, args []interface{}) (expr, error) {
	if len(args) != 2 {
		return nil, NewRqlError(ErrRQLWrong, "Expected only two arguments for '%s' rql function but founded '%d'", "lt", len(args))
	}
	return ctx.fieldExpr(args, ctx.sqlOpSimple("<"))
}
func le(ctx *context, args []interface{}) (expr, error) {
	if len(args) != 2 {
		return nil, NewRqlError(ErrRQLWrong, "Expected only two arguments for '%s' rql function but founded '%d'", "le", len(args))
	}
	return ctx.fieldExpr(args, ctx.sqlOpSimple("<="))
}
func gt(ctx *context, args []interface{}) (expr, error) {
	if len(args) != 2 {
		return nil, NewRqlError(ErrRQLWrong, "Expected only two arguments for '%s' rql function but founded '%d'", "gt", len(args))
	}
	return ctx.fieldExpr(args, ctx.sqlOpSimple(">"))
}
func ge(ctx *context, args []interface{}) (expr, error) {
	if len(args) != 2 {
		return nil, NewRqlError(ErrRQLWrong, "Expected only two arguments for '%s' rql function but founded '%d'", "ge", len(args))
	}
	return ctx.fieldExpr(args, ctx.sqlOpSimple(">="))
}
func like(ctx *context, args []interface{}) (expr, error) {
	if len(args) != 2 {
		return nil, NewRqlError(ErrRQLWrong, "Expected only two arguments for '%s' rql function but founded '%d'", "like", len(args))
	}
	return ctx.fieldExpr(args, ctx.sqlOpSimple("LIKE "))
}

func (st *SqlTranslator) sort(tableAlias string, root *data.Node) (string, error) {
	var b bytes.Buffer
	sorts := st.rootNode.Sort()
	for i := range sorts {
		f := root.Meta.FindField(sorts[i].By)
		if f == nil {
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

func (st *SqlTranslator) query(tableAlias string, root *data.Node) (*SqlQuery, error) {
	ctx := &context{root: root, tblAlias: tableAlias, binds: make([]interface{}, 0)}
	whereExp, err := ctx.nodeToOpExpr(st.rootNode.Node)
	if err != nil {
		return nil, err
	}

	sort, err := st.sort(tableAlias, root)
	if err != nil {
		return nil, err
	}

	return &SqlQuery{Where: whereExp(), Binds: ctx.binds, Sort: sort, Limit: st.rootNode.Limit(), Offset: st.rootNode.Offset()}, nil
}
