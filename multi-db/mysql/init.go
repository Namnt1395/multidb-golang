package mysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"setting-dsp"
)

const (
	Driver      = "mysql"
	UsernameDb1 = "namnt"
	PasswordDb1 = "123456"
	HostDb1     = "localhost"
	Database1   = "bg_dsp4"

	UsernameDb2 = "namnt"
	PasswordDb2 = "123456"
	HostDb2     = "localhost"
	Database2   = "bg_email"
)

var dsnDb1 = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", UsernameDb1, PasswordDb1, HostDb1, 3306, Database1)
var dsnDb2 = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", UsernameDb2, PasswordDb2, HostDb2, 3306, Database2)
var mysqlConDb1 *sql.DB
var mysqlConDb2 *sql.DB

const (
	RealTimeAerTag = 4
	RealTimeAerAd  = 5
)

func ConnectMysqlDb1() {
	//TODO connect mysql
	var e1 error
	mysqlConDb1, e1 = sql.Open(Driver, dsnDb1)
	if e1 != nil {
		fmt.Println("MYSQL connect error: " + e1.Error())
		panic("END")
	}
}
func ConnectMysqlDb2() {
	//TODO connect mysql
	var e1 error
	mysqlConDb2, e1 = sql.Open(Driver, dsnDb2)
	if e1 != nil {
		fmt.Println("MYSQL connect error: " + e1.Error())
		panic("END")
	}
}
func ContinueConnectMySQLDb1() {
	err := mysqlConDb1.Ping()
	if err != nil {
		fmt.Println(err.Error())
		mysqlConDb1, _ = sql.Open(setting_dsp.MysqlDriver, dsnDb1)
	}
}
func ContinueConnectMySQLDb2() {
	err := mysqlConDb2.Ping()
	if err != nil {
		fmt.Println(err.Error())
		mysqlConDb2, _ = sql.Open(setting_dsp.MysqlDriver, dsnDb1)
	}
}
func CloseDb1() {
	e1 := mysqlConDb1.Close()
	if e1 != nil {
		fmt.Println("ERROR e1: ", e1.Error())
	}
}
func CloseDb2() {
	e1 := mysqlConDb2.Close()
	if e1 != nil {
		fmt.Println("ERROR e1: ", e1.Error())
	}
}
