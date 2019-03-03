package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strconv"
)

const cnareaTableName = "cnarea_2017"

type cnareaData struct {
	level, areaCode, zipCode, cityCode int
	name, shortName, mergerName string
	lng, lat float64
}

func queryDb() {
	db, err := sql.Open("mysql", "root:123456@localhost/cnarea?charset=utf8")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 统计条数
	countSql := fmt.Sprintf("SELECT count(1) num FROM %s", cnareaTableName)
	var num string
	row := db.QueryRow(countSql)
	err = row.Scan(&num)
	if err != nil {
		log.Fatal(err)
	}
	dataNumber, err := strconv.Atoi(num)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s 共计%d条数据\n", cnareaTableName, dataNumber)

	var data []map[string]string

	// 查询
	querySql := fmt.Sprintf("SELECT level, area_code, zip_code, city_code, name, short_name, merger_name, lng, lat FROM %s", cnareaTableName)
	rows, err := db.Query(querySql)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var level, areaCode, zipCode, cityCode, name, shortName, mergerName, lng, lat string
		if err := rows.Scan(&level, &areaCode, &zipCode, &cityCode, &name, &shortName, &mergerName, &lng, &lat); err != nil {
			log.Fatal(err)
		}
		row := make(map[string]string)
		row["level"] = level
		row["areaCode"] = areaCode
		row["zipCode"] = zipCode
		row["cityCode"] = cityCode
		row["mergerName"] = mergerName
		row["name"] = name
		row["shortName"] = shortName
		row["lng"] = lng
		row["lat"] = lat
		data = append(data, row)
	}
	return data, dataNumber

}

func main() {
	//dataFile, err := os.Create("../data/cnarea/cnarea_2017.data")
	//if err != nil {
	//	log.Fatalf("create cnarea data fail: %s", err)
	//}
	//enc := gob.NewEncoder(dataFile)
	//
	queryDb()
}
