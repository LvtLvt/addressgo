package main

import (
	"csvgo/data"
	"csvgo/data/meta"
	"os"
	"sync"
)

var pwd, _ = os.Getwd()
var dest = pwd + "/result"

var 주소 = data.FilePhase{
	PrefixName: "주소",
	CsvHead:    meta.CsvHead["주소"],
	IdFieldIdx: 0,
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

var aggregator data.Collector = &data.FileCollector{
	Src:          pwd + "/juso",
	Dest:         dest,
	FilePhases:   []data.FilePhase{주소, 지번, 부가정보, 개선},
	FromEncoding: "CP949",
}

func main() {

	os.RemoveAll(dest)

	aggregator.Collect()

	numOfShards := 5

	var wg sync.WaitGroup
	source := dest
	wg.Add(4)
	jusoSorter := data.NewFileSorter(source, source+"/chunks", numOfShards, 5)
	jibunSorter := data.NewFileSorter(source, source+"/chunks", numOfShards, 5)
	bugaSorter := data.NewFileSorter(source, source+"/chunks", numOfShards, 5)
	doroSorter := data.NewFileSorter(source, source+"/chunks", numOfShards, 5)

	jusoSorter.OnComplete = func() { wg.Done() }
	jibunSorter.OnComplete = func() { wg.Done() }
	bugaSorter.OnComplete = func() { wg.Done() }
	doroSorter.OnComplete = func() { wg.Done() }

	go jusoSorter.Sort(주소)
	go jibunSorter.Sort(지번)
	go bugaSorter.Sort(부가정보)
	go doroSorter.Sort(개선)

	wg.Wait()
}
