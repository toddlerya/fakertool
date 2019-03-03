package main

import (
	"database/sql"
	"encoding/gob"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"strconv"
)

const dataPath = "data/cnarea/cnarea_2017.data"
const cnareaTableName = "cnarea_2017"

// gob 需要序列化的 struct 的属性应该是public的，即应该是大写字母开头！！！
// gob 需要序列化的 struct 的属性应该是public的，即应该是大写字母开头！！！
// gob 需要序列化的 struct 的属性应该是public的，即应该是大写字母开头！！！
type cnareaData struct {
	Level, AreaCode, ZipCode, CityCode, Name, ShortName, MergerName, Lng, Lat interface{}
}

func extractMySQL() ([]map[string]interface{}, int) {
	db, err := sql.Open("mysql", "root:123456@tcp(localhost:3306)/cnarea?charset=utf8")
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

	var data []map[string]interface{}

	// 查询
	querySql := fmt.Sprintf(
		"SELECT "+
			"level, area_code, zip_code, city_code, name, short_name, merger_name, lng, lat "+
			"FROM %s", cnareaTableName)
	rows, err := db.Query(querySql)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var level, areaCode, zipCode, cityCode, name, shortName, mergerName, lng, lat string
		if err := rows.Scan(&level, &areaCode, &zipCode, &cityCode, &name, &shortName, &mergerName, &lng, &lat); err != nil {
			log.Fatal(err)
		}
		row := make(map[string]interface{})
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

func serialization(data []map[string]interface{}) {
	dataFile, err := os.Create(dataPath)
	if err != nil {
		log.Fatalf("create cnarea data fail: %s", err)
	}

	var loadData []cnareaData
	for _, row := range data {
		eachRow := cnareaData{
			Level:      row["level"],
			AreaCode:   row["areaCode"],
			ZipCode:    row["zipCode"],
			CityCode:   row["cityCode"],
			Name:       row["name"],
			ShortName:  row["shortName"],
			MergerName: row["mergerName"],
			Lng:        row["lng"],
			Lat:        row["lat"],
		}
		loadData = append(loadData, eachRow)
	}

	gob.Register(cnareaData{})
	encoder := gob.NewEncoder(dataFile)
	err = encoder.Encode(loadData)
	if err != nil {
		log.Fatalf("serialization fail: %s", err)
	}
}

func main() {
	cnareaData, _ := extractMySQL()
	serialization(cnareaData)

}
