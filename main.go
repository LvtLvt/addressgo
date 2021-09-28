package main

import (
	"csvgo/data"
	"csvgo/data/meta"
	"encoding/csv"
	"fmt"
	"github.com/gocarina/gocsv"
	"io"
	"os"
	"runtime"
)

var pwd, _ = os.Getwd()
var dest = pwd + "/result"

var 주소 = data.FilePhase{
	PrefixName: "주소",
	CsvHead:    meta.CsvHead["주소"],
}

var 지번 = data.FilePhase{
	PrefixName: "지번",
	CsvHead:    meta.CsvHead["지번"],
}

var 부가정보 = data.FilePhase{
	PrefixName: "부가정보",
	CsvHead:    meta.CsvHead["부가정보"],
}

var 개선 = data.FilePhase{
	PrefixName: "개선",
	CsvHead:    meta.CsvHead["개선"],
}

var aggregator data.Aggregator = &data.FileAggregator{
	Src:          pwd + "/juso",
	Dest:         dest,
	FilePhases:   []data.FilePhase{주소, 지번, 부가정보, 개선},
	FromEncoding: "euc-kr",
}

func main() {

	os.RemoveAll(dest)

	aggregator.Aggregate()

	println("finished....")
	// TODO: indexing

	gocsv.SetCSVReader(func(in io.Reader) gocsv.CSVReader {
		r := csv.NewReader(in)
		r.LazyQuotes = true
		r.Comma = '|'
		return r
	})

	//var jusoArr []*meta.Juso
	//jusoArr := []*meta.Juso{}

	jusoFile, _ := os.OpenFile(dest+"/"+주소.GetFilename(), data.DefaultFileFlag, data.DefaultFileMode)
	info, _ := jusoFile.Stat()
	print(info.Size())
	c := make(chan meta.Juso)

	go func() {
		err := gocsv.UnmarshalToChan(jusoFile, c)
		if err != nil {
			println(err.Error())
			return
		}
	}()

	for range c {

	}

	//err := gocsv.UnmarshalFile(jusoFile, &jusoArr)
	//if err != nil {
	//	print(err.Error())
	//	return
	//}
	//
	//println(len(jusoArr))

	PrintMemUsage()

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

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
