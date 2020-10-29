package mysql

import (
	"database/sql"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var Debug bool
var DbConnection *sql.DB

func init() {
	Debug = false // default to false
}

// Result holds the results of a query as map[string]interface{}
type Result map[string]interface{}

type Query struct {

	// Database - database name and primary key, set with New()
	tableName  string
	primaryKey string

	// SQL - Private fields used to store sql before building sql query
	sql    string
	sel    []string
	from   string
	update string
	join   string
	where  string
	group  string
	having string
	order  string
	offset string
	limit  string

	// Extra args to be substituted in the *where* clause
	args []interface{}
}

// New builds a new Query, given the table and primary key
func New(t string, pk string, db ...string) *Query {
	switch db[0] {
	case Database1:
		if mysqlConDb1 == nil {
			return nil
		}
		DbConnection = mysqlConDb1
		break
	case Database2:
		if mysqlConDb2 == nil {
			return nil
		}
		DbConnection = mysqlConDb2
		break
	default:
		if mysqlConDb1 == nil {
			return nil
		}
		DbConnection = mysqlConDb1
	}
	q := &Query{
		tableName:  t,
		primaryKey: pk,
	}

	return q
}
func (q *Query) SetData(data map[string]interface{}, object interface{}) interface{} {
	result := make(map[string]interface{})
	st := reflect.TypeOf(object)
	num := st.NumField()
	// for 1
	for i := 0; i < num; i++ {
		item := st.Field(i)
		// for in data
		for v, _ := range data {
			// check theo tag
			builderName := item.Tag.Get("builder")
			builder := strings.Replace(builderName, ",omit", "", -1)
			builder = strings.Replace(builder, ",", "", -1)
			if builder == v {
				// switch
				switch item.Type.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					format := fmt.Sprintf("%v", data[v])
					result[item.Name], _ = strconv.Atoi(format)
				case reflect.String:
					result[item.Name] = fmt.Sprintf("%v", data[v])
				case reflect.Float64:
					format := fmt.Sprintf("%v", data[v])
					result[item.Name], _ = strconv.ParseFloat(format, 64)
				default:
					result[item.Name] = data[v]
				}
			}
		}
	}
	mapstructure.Decode(result, &object)
	return object
}

// Insert inserts a record in the database
func (q *Query) Insert(params map[string]interface{}) (int64, error) {
	// Insert and retrieve ID in one step from db
	sql := q.formatInsertSQL(params)
	if Debug {
		fmt.Printf("INSERT SQL:%s %v\n", sql, valuesFromParams(params))
	}
	id, err := Insert(sql, valuesFromParams(params)...)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// Insert a object in the database
func (q *Query) InsertObject(object interface{}) (int64, error) {
	var params = make(map[string]interface{})
	////--- Extract Value without specifying Type
	val := reflect.Indirect(reflect.ValueOf(object))
	for i := 0; i < val.Type().NumField(); i++ {
		// create map param
		builder := val.Type().Field(i).Tag.Get("builder")
		if builder != "" && !strings.Contains(builder, "omit") {
			// switch
			switch val.Field(i).Type().Kind() {
			case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
				params[val.Type().Field(i).Tag.Get("builder")] = val.Field(i).Int()
			case reflect.String:
				params[val.Type().Field(i).Tag.Get("builder")] = val.Field(i).String()
			case reflect.Float32:
				params[val.Type().Field(i).Tag.Get("builder")] = val.Field(i).Float()
			case reflect.Float64:
				params[val.Type().Field(i).Tag.Get("builder")] = val.Field(i).Float()
			default:
			}

		}
	}
	// Insert and retrieve ID in one step from db
	sql := q.formatInsertSQL(params)
	if Debug {
		fmt.Printf("INSERT SQL:%s %v\n", sql, valuesFromParams(params))
	}
	id, err := Insert(sql, valuesFromParams(params)...)
	if err != nil {
		return 0, err
	}

	return id, nil
}

func (q *Query) formatInsertSQL(params map[string]interface{}) string {
	var cols, vals []string
	for i, k := range sortedParamKeys(params) {
		cols = append(cols, QuoteField(k))
		vals = append(vals, Placeholder(i+1))
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)", q.tableName, strings.Join(cols, ","), strings.Join(vals, ","))
	return query
}

// Update one model specified in this query - the column names MUST be verified in the model
func (q *Query) Update(params map[string]interface{}) (int64, error) {
	return q.UpdateAll(params)
}

// UpdateAll updates all models specified in this relation
func (q *Query) UpdateAll(params map[string]interface{}) (int64, error) {
	// Create sql for update from ALL params
	q.UpdateSql(fmt.Sprintf("UPDATE %s SET %s", q.table(), querySQL(params)))
	q.args = append(valuesFromParams(params), q.args...)
	if Debug {
		fmt.Printf("UPDATE SQL:%s\n%v\n", q.QueryString(), valuesFromParams(params))
	}
	rs, err := q.Result()
	id, err := rs.RowsAffected()
	return id, err
}

// DeleteAll delets *all* models specified in this relation
func (q *Query) DeleteAll() error {
	q.Select(fmt.Sprintf("DELETE FROM %s", q.table()))
	if Debug {
		fmt.Printf("DELETE SQL:%s <= %v\n", q.QueryString(), q.args)
	}
	// Execute
	_, err := q.Result()
	return err
}

// Count fetches a count of model objects (executes SQL).
func (q *Query) Count() (int64, error) {
	// Store the previous select and set
	s := q.sel
	countSelect := fmt.Sprintf("SELECT COUNT(%s) FROM %s", q.pk(), q.table())
	q.Select(countSelect)
	o := strings.Replace(q.order, "ORDER BY ", "", 1)
	q.order = ""
	// Fetch count from db for our sql with count select and no order set
	var count int64
	rows, err := q.Rows()
	if err != nil {
		return 0, fmt.Errorf("Error querying database for count: %s\nQuery:%s", err, q.QueryString())
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			return 0, err
		}
	}

	// Reset select after getting count query
	q.Select(s...)
	q.Order(o)
	q.reset()

	return count, err
}

// Result executes the query against the database, returning sql.Result, and error (no rows)
// (Executes SQL)
func (q *Query) Result() (sql.Result, error) {
	results, err := Exec(q.QueryString(), q.args...)
	return results, err
}

// Rows executes the query against the database, and return the sql rows result for this query
func (q *Query) Rows() (*sql.Rows, error) {
	results, err := QuerySql(q.QueryString(), q.args...)
	return results, err
}

// FirstResult executes the SQL and returrns the first result
func (q *Query) FirstResult() (Result, error) {
	// Set a limit on the query
	q.Limit(1)
	// Fetch all results (1)
	results, err := q.Results()
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("%s", "No results")
	}
	// Return the first result
	return results[0], nil
}

// Results returns an array of results
func (q *Query) Results() ([]Result, error) {
	// Make an empty result set map
	var results []Result
	rows, err := q.Rows()
	if err != nil {
		return results, fmt.Errorf("Error querying database for rows: %s\nQUERY:%s", err, q)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return results, fmt.Errorf("Error fetching columns: %s\nQUERY:%s\nCOLS:%s", err, q, cols)
	}
	for rows.Next() {
		result, err := ScanRow(cols, rows)
		if err != nil {
			return results, fmt.Errorf("Error fetching row: %s\nQUERY:%s\nCOLS:%s", err, q, cols)
		}
		results = append(results, result)
	}
	return results, nil
}

func (q *Query) ResultsSimple() (*sql.Rows, []string, error) {
	rows, err := q.Rows()
	cols := make([]string, 0)
	if err != nil {
		return rows, cols, fmt.Errorf("Error querying database for rows: %s\nQUERY:%s", err, q)
	}
	cols, err = rows.Columns()
	if err != nil {
		return rows, cols, fmt.Errorf("Error fetching columns: %s\nQUERY:%s\nCOLS:%s", err, q, cols)
	}
	return rows, cols, nil
}

// QueryString builds a query string to use for results
func (q *Query) QueryString() string {
	if q.sql == "" {
		selectSlice := make([]string, len(q.sel))
		for i, v := range q.sel {
			selectSlice[i] = fmt.Sprintf("%s", trim(v))
		}
		selectSql := ""
		if len(q.sel) <= 0 {
			selectSql = fmt.Sprintf("SELECT %s.* FROM %s", q.table(), q.table())
		} else {
			selectSql = fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectSlice, ","), q.table())
		}
		if len(q.update) > 0 {
			selectSql = q.update
		}
		q.sql = fmt.Sprintf("%s %s %s %s %s %s %s %s", selectSql, q.join, q.where, q.group, q.having, q.order, q.limit, q.offset)
		q.sql = strings.TrimRight(q.sql, " ")
		q.sql = strings.Replace(q.sql, "  ", " ", -1)
		q.sql = strings.Replace(q.sql, "   ", " ", -1)
		// Replace ? with whatever placeholder db prefers
		q.replaceArgPlaceholders()

		q.sql = q.sql + ";"
		fmt.Println("sql..", q.sql)
	}

	return q.sql
}

// Limit sets the sql LIMIT with an int
func (q *Query) Limit(limit int) *Query {
	q.limit = fmt.Sprintf("LIMIT %d", limit)
	q.reset()
	return q
}

// Offset sets the sql OFFSET with an int
func (q *Query) Offset(offset int) *Query {
	q.offset = fmt.Sprintf("OFFSET %d", offset)
	q.reset()
	return q
}

// Where defines a WHERE clause on SQL - Additional calls add WHERE () AND () clauses
func (q *Query) Where(args ...interface{}) *Query {
	var paramSlice []string
	if args != nil {
		for i, param := range args {
			if i == 2 {
				switch i := param.(type) {
				case string:
					paramSlice = append(paramSlice, fmt.Sprintf("%s%s%s", "'", param.(string), "'"))
				case int:
					paramSlice = append(paramSlice, strconv.Itoa(i))
				case int64:
					paramSlice = append(paramSlice, strconv.FormatInt(i, 10))
				case float32:
					paramSlice = append(paramSlice, fmt.Sprint(i))
				case float64:
					paramSlice = append(paramSlice, fmt.Sprint(i))
				case bool:
					paramSlice = append(paramSlice, strconv.FormatBool(i))
				default:
					paramSlice = append(paramSlice, param.(string))
				}
			} else {
				paramSlice = append(paramSlice, param.(string))
			}

		}
	}
	if len(q.where) > 0 {
		q.where = fmt.Sprintf("%s AND (%s)", q.where, strings.Join(paramSlice, ""))
	} else {
		q.where = fmt.Sprintf(" WHERE (%s)", strings.Join(paramSlice, ""))
	}
	q.reset()
	return q
}

// Where defines a WHERE clause on SQL - Additional calls add WHERE () AND () clauses
func (q *Query) AndWhere(args ...interface{}) *Query {
	return q.Where(args...)
}

// OrWhere defines a where clause on SQL - Additional calls add WHERE () OR () clauses
func (q *Query) OrWhere(args ...interface{}) *Query {

	var paramSlice []string
	if args != nil {
		for i, param := range args {
			if i == 2 {
				switch i := param.(type) {
				case string:
					paramSlice = append(paramSlice, fmt.Sprintf("%s%s%s", "'", param.(string), "'"))
				case int:
					paramSlice = append(paramSlice, strconv.Itoa(i))
				case float32:
					paramSlice = append(paramSlice, fmt.Sprint(i))
				case float64:
					paramSlice = append(paramSlice, fmt.Sprint(i))
				case bool:
					paramSlice = append(paramSlice, strconv.FormatBool(i))
				default:
					paramSlice = append(paramSlice, param.(string))
				}
			} else {
				paramSlice = append(paramSlice, param.(string))
			}

		}
	}
	if len(q.where) > 0 {
		q.where = fmt.Sprintf("%s OR (%s)", q.where, strings.Join(paramSlice, ""))
	} else {
		q.where = fmt.Sprintf("WHERE (%s)", strings.Join(paramSlice, ""))
	}

	q.reset()
	return q
}

// WhereIn adds a Where clause which selects records IN() the given array
func (q *Query) WhereIn(col string, args string) *Query {
	// Return no results, so that when chaining callers
	// don't have to check for empty arrays
	if len(args) == 0 {
		q.Limit(0)
		q.reset()
		return q
	}
	paramSlice := strings.Split(args, ",")
	in := ""
	for _, param := range paramSlice {
		if _, err := strconv.Atoi(param); err == nil {
			paramInt, _ := strconv.Atoi(param)
			in = fmt.Sprintf("%s%d,", in, paramInt)
		} else {
			in = fmt.Sprintf("%s%s%s%s,", in, "'", param, "'")
		}
	}
	in = strings.TrimRight(in, ",")
	sql := fmt.Sprintf("%s IN (%s)", col, in)
	if len(q.where) > 0 {
		q.where = fmt.Sprintf("%s AND (%s)", q.where, sql)
	} else {
		q.where = fmt.Sprintf("WHERE (%s)", sql)
	}
	q.reset()
	return q
}

// WhereIn adds a Where clause which selects records IN() the given array
func (q *Query) OrWhereIn(col string, args string) *Query {
	// Return no results, so that when chaining callers
	// don't have to check for empty arrays
	if len(args) == 0 {
		q.Limit(0)
		q.reset()
		return q
	}
	paramSlice := strings.Split(args, ",")
	in := ""
	for _, param := range paramSlice {
		if _, err := strconv.Atoi(param); err == nil {
			paramInt, _ := strconv.Atoi(param)
			in = fmt.Sprintf("%s%d,", in, paramInt)
		} else {
			in = fmt.Sprintf("%s%s%s%s,", in, "'", param, "'")
		}
	}
	in = strings.TrimRight(in, ",")
	sql := fmt.Sprintf("%s IN (%s)", col, in)
	if len(q.where) > 0 {
		// ex: string (device1 IN (3187)) remove ')' at the end to make (device1 IN (3187) or device2 IN (3192))
		q.where = fmt.Sprintf("%s OR %s)", strings.TrimSuffix(q.where, ")"), sql)
	} else {
		q.where = fmt.Sprintf("WHERE (%s)", sql)
	}
	q.reset()
	return q
}

func (q *Query) InnerJoin(args ...interface{}) *Query {
	var paramSlice []string
	var tableJoin string
	if args != nil {
		for i, param := range args {
			if i == 0 {
				tableJoin = param.(string)
			} else {
				paramSlice = append(paramSlice, param.(string))
			}
		}
	}
	sql := fmt.Sprintf("INNER JOIN %s ON %s", tableJoin, strings.Join(paramSlice, ""))
	if len(q.join) > 0 {
		q.join = fmt.Sprintf("%s %s", q.join, sql)
	} else {
		q.join = fmt.Sprintf("%s", sql)
	}
	q.reset()
	return q
}
func (q *Query) LeftJoin(args ...interface{}) *Query {
	var paramSlice []string
	var tableJoin string
	if args != nil {
		for i, param := range args {
			if i == 0 {
				tableJoin = param.(string)
			} else {
				paramSlice = append(paramSlice, param.(string))
			}
		}
	}
	sql := fmt.Sprintf("LEFT JOIN %s ON %s", tableJoin, strings.Join(paramSlice, ""))
	if len(q.join) > 0 {
		q.join = fmt.Sprintf("%s %s", q.join, sql)
	} else {
		q.join = fmt.Sprintf("%s", sql)
	}
	q.reset()
	return q
}
func (q *Query) RightJoin(args ...interface{}) *Query {
	var paramSlice []string
	var tableJoin string
	if args != nil {
		for i, param := range args {
			if i == 0 {
				tableJoin = param.(string)
			} else {
				paramSlice = append(paramSlice, param.(string))
			}
		}
	}
	sql := fmt.Sprintf("RIGHT JOIN %s ON %s", tableJoin, strings.Join(paramSlice, ""))
	if len(q.join) > 0 {
		q.join = fmt.Sprintf("%s %s", q.join, sql)
	} else {
		q.join = fmt.Sprintf("%s", sql)
	}
	q.reset()
	return q
}
func (q *Query) FullJoin(args ...interface{}) *Query {
	var paramSlice []string
	var tableJoin string
	if args != nil {
		for i, param := range args {
			if i == 0 {
				tableJoin = param.(string)
			} else {
				paramSlice = append(paramSlice, param.(string))
			}
		}
	}
	sql := fmt.Sprintf("FULL OUTER JOIN %s ON %s", tableJoin, strings.Join(paramSlice, ""))
	if len(q.join) > 0 {
		q.join = fmt.Sprintf("%s %s", q.join, sql)
	} else {
		q.join = fmt.Sprintf("%s", sql)
	}
	q.reset()
	return q
}

// Order defines ORDER BY sql
func (q *Query) Order(sql string) *Query {
	if sql == "" {
		q.order = ""
	} else {
		q.order = fmt.Sprintf("ORDER BY %s", sql)
	}
	q.reset()

	return q
}

// Group defines GROUP BY sql
func (q *Query) Group(sql string) *Query {
	if sql == "" {
		q.group = ""
	} else {
		q.group = fmt.Sprintf("GROUP BY %s", sql)
	}
	q.reset()
	return q
}

// Having defines HAVING sql
func (q *Query) Having(sql string) *Query {
	if sql == "" {
		q.having = ""
	} else {
		q.having = fmt.Sprintf("HAVING %s", sql)
	}
	q.reset()
	return q
}

//setting from table
func (q *Query) From(from string) *Query {
	q.from = from
	q.reset()
	return q
}

// Select defines SELECT  sql
func (q *Query) Select(field ...string) *Query {
	q.sel = field
	q.reset()
	return q
}

//add select
func (q *Query) AddSelect(field ...string) *Query {
	for _, item := range field {
		q.sel = append(q.sel, item)
	}
	q.reset()
	return q
}

// Select defines Update  sql
func (q *Query) UpdateSql(field string) *Query {
	q.update = field
	q.reset()
	return q
}
func (q *Query) ResetSelect() *Query {
	q.sel = nil
	return q
}

// Clear sql/query caches
func (q *Query) reset() {
	// clear stored sql
	q.sql = ""
}

// Ask model for primary key name to use
func (q *Query) pk() string {
	return QuoteField(q.primaryKey)
}

// Ask model for table name to use
func (q *Query) table() string {
	if len(q.from) > 0 {
		return q.from
	}
	return QuoteField(q.tableName)
}

// Replace ?
func (q *Query) replaceArgPlaceholders() {
	// Match ? and replace with argument placeholder from database
	for i := range q.args {
		q.sql = strings.Replace(q.sql, "?", Placeholder(i+1), 1)
	}
}

// Sorts the param names given - map iteration order is explicitly random in Go
// Need params in a defined order to avoid unexpected results.
func sortedParamKeys(params map[string]interface{}) []string {
	sortedKeys := make([]string, len(params))
	i := 0
	for k := range params {
		sortedKeys[i] = k
		i++
	}
	sort.Strings(sortedKeys)

	return sortedKeys
}

// Generate a set of values for the params in order
func valuesFromParams(params map[string]interface{}) []interface{} {
	var values []interface{}
	for _, key := range sortedParamKeys(params) {
		values = append(values, params[key])
	}
	return values
}

// Used for update statements, turn params into sql i.e. "col"=?
func querySQL(params map[string]interface{}) string {
	var output []string
	for _, k := range sortedParamKeys(params) {
		output = append(output, fmt.Sprintf("%s=?", QuoteField(k)))
	}
	return strings.Join(output, ",")
}

func ScanRow(cols []string, rows *sql.Rows) (Result, error) {
	// We return a map[string]interface{} for each row scanned
	result := Result{}
	values := make([]interface{}, len(cols))
	for i := 0; i < len(cols); i++ {
		var col interface{}
		values[i] = &col
	}
	// Scan results into these interfaces
	err := rows.Scan(values...)
	if err != nil {
		return nil, fmt.Errorf("Error scanning row: %s", err)
	}

	for i := 0; i < len(cols); i++ {
		v := *values[i].(*interface{})
		if values[i] != nil {
			switch v.(type) {
			default:
				result[cols[i]] = v
			case bool:
				result[cols[i]] = v.(bool)
			case int:
				result[cols[i]] = int64(v.(int))
			case []byte: // text cols are given as bytes
				result[cols[i]] = string(v.([]byte))
			case int64:
				result[cols[i]] = v.(int64)
			}
		}

	}
	return result, nil
}
func trim(str string) string {
	re := regexp.MustCompile(`[\s]+`)
	// replace multi space = 1 space
	str = re.ReplaceAllString(str, " ")

	return strings.TrimSpace(str)
}
