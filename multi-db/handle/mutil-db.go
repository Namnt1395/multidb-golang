package handle

import (
	"fmt"
	"multi-db/mysql"
)

func InsertMultiDb() {
	mysql.ConnectMysqlDb1()
	mysql.ConnectMysqlDb2()
	// Get data from db1
	queryDb1 := mysql.AdsTagQuery().Select("*")
	rsDb1, err1 := queryDb1.Results()
	if err1 != nil {
		fmt.Println(err1.Error())
	}
	for _, v := range rsDb1 {
		data := mysql.AdsTagQuery().SetData(v, mysql.AdsTagModel{}).(mysql.AdsTagModel)
		// insert to db 2
		_, err2 := mysql.AdsTagCopyQuery().InsertObject(mysql.AdsTagCopyModel{
			AdId:       data.AdId,
			ContentTag: data.ContentTag,
		})
		if err2 != nil {
			fmt.Println(err1.Error())
		}
	}

	mysql.CloseDb1()
	mysql.CloseDb2()
}
