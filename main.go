package main

import (
	"csvgo/data"
	"csvgo/data/meta"
)

/**
address metadata
csv parser
csv writer
*/

func main() {

	var aggregator data.Aggregator = &data.FileAggregator{
		Src:  "juso",
		Dest: "result",
		FilePhases: []data.FilePhase{
			{
				PrefixName: "주소",
				CsvHead:    meta.CsvHead["주소"],
			},
			{
				PrefixName: "지번",
				CsvHead:    meta.CsvHead["지번"],
			},
			{
				PrefixName: "부가정보",
				CsvHead:    meta.CsvHead["부가정보"],
			},
			{
				PrefixName: "개선",
				CsvHead:    meta.CsvHead["개선"],
			},
		},
		FromEncoding: "euc-kr",
	}

	aggregator.Aggregate()

	//file, err := os.OpenFile("주소.txt", os.O_RDWR|os.O_CREATE, os.ModePerm)
	//if err != nil {
	//	panic(err)
	//}
	//defer file.Close()
	//
	//jusoArr := []*Juso{}
	//
	//gocsv.SetCSVReader(func(in io.Reader) gocsv.CSVReader {
	//	r := csv.NewReader(in)
	//	r.LazyQuotes = true
	//	r.Comma = '|'
	//	return r
	//})
	//
	//if err := gocsv.UnmarshalFile(file, &jusoArr); err != nil {
	//	panic(err)
	//}
	//
	//println(len(jusoArr))
	//
	////reader, err := iconv.NewReader(file, "euc-kr", "utf-8")
	////if err != nil {
	////	return
	////}
	//
	////os.WriteFile("result.txt", os.O_RDWR|os.O_CREATE, os.ModePerm)
	//
	//println("end...")
	//
	//jusoArr = jusoArr[0:6]
}
