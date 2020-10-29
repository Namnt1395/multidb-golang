package mysql

type TestBgModel struct {
	Id  int `builder:"id"`
	Age int `builder:"age"`
}

func TestBgQuery() *Query {
	return New("test_bg", "id", Database2)
}
