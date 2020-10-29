package mysql

type TestModel struct {
	Id   int    `builder:"id"`
	Name string `builder:"name"`
}

func TestQuery() *Query {
	return New("test", "id", Database1)
}
