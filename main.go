package main

import (
	"encoding/csv"
	"github.com/gocarina/gocsv"
	"io"
	"os"
)

/**
address metadata
csv parser
csv writer
*/

// 주소
type Juso struct {
	관리번호     string `csv:"관리번호"`
	도로명코드    string `csv:"도로명코드"`
	읍면동일련번호  string `csv:"읍면동일련번호"`
	지하여부     string `csv:"지하여부"`
	건물본번     string `csv:"건물본번"`
	건물부번     string `csv:"건물부번"`
	기초구역번호   string `csv:"기초구역번호"`
	변경사유코드   string `csv:"변경사유코드"`
	고시일자     string `csv:"고시일자"`
	변경전도로명주소 string `csv:"변경전도로명주소"`
	상세주소부여여부 string `csv:"상세주소부여여부"`
}

type Doro struct {
	도로명코드   string `csv:"도로명코드"`
	도로명     string `csv:"도로명"`
	도로명로마자  string `csv:"도로명로마자"`
	읍면동일련번호 string `csv:"읍면동일련번호"`
	시도명     string `csv:"시도명"`
	시도로마자   string `csv:"시도로마자"`
	시군구명    string `csv:"시군구명"`
	시군구로마자  string `csv:"시군구로마자"`
	읍면동명    string `csv:"읍면동명"`
	읍면동로마자  string `csv:"읍면동로마자"`
	읍면동구분   string `csv:"읍면동구분"`
	읍면동코드   string `csv:"읍면동코드"`
	사용여부    string `csv:"사용여부"`
	변경사유    string `csv:"변경사유"`
	변경이력정보  string `csv:"변경이력정보"`
	고시일자    string `csv:"고시일자"`
	말소일자    string `csv:"말소일자"`
}

type Buga struct {
	관리번호     string `csv:"관리번호"`
	행정동코드    string `csv:"행정동코드"`
	행정동명     string `csv:"행정동명"`
	우편번호     string `csv:"우편번호"`
	우편번호일련번호 string `csv:"우편번호일련번호"`
	다량배달처명   string `csv:"다량배달처명"`
	건축물대장건물명 string `csv:"건축물대장건물명"`
	시군구건물명   string `csv:"시군구건물명"`
	공동주택여부   string `csv:"공동주택여부"`
}

type Jibun struct {
	관리번호   string `csv:"관리번호"`
	일련번호   string `csv:"일련번호"`
	법정동코드  string `csv:"법정동코드"`
	시도명    string `csv:"시도명"`
	시군구명   string `csv:"시군구명"`
	법정읍면동명 string `csv:"법정읍면동명"`
	법정리명   string `csv:"법정리명"`
	산여부    string `csv:"산여부"`
	지번본번   string `csv:"지번본번"`
	지번부번   string `csv:"지번부번"`
	대표여부   string `csv:"대표여부"`
}

func main() {

	file, err := os.OpenFile("주소.txt", os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	jusoArr := []*Juso{}

	gocsv.SetCSVReader(func(in io.Reader) gocsv.CSVReader {
		r := csv.NewReader(in)
		r.LazyQuotes = true
		r.Comma = '|'
		return r
	})

	if err := gocsv.UnmarshalFile(file, &jusoArr); err != nil {
		panic(err)
	}

	println(len(jusoArr))

	//reader, err := iconv.NewReader(file, "euc-kr", "utf-8")
	//if err != nil {
	//	return
	//}

	//os.WriteFile("result.txt", os.O_RDWR|os.O_CREATE, os.ModePerm)

	println("end...")

	jusoArr = jusoArr[0:6]
}
