package main

import (
	"csvgo/data"
	"csvgo/data/meta"
	"os"
)

var pwd, _ = os.Getwd()
var dest = pwd + "/result"

var juso = data.FilePhase{
	PrefixName: "주소",
	CsvHead:    meta.CsvHead["주소"],
	IdFieldIdx: 0,
}

var jibun = data.FilePhase{
	PrefixName: "지번",
	CsvHead:    meta.CsvHead["지번"],
	IdFieldIdx: 0,
}

var buga = data.FilePhase{
	PrefixName: "부가정보",
	CsvHead:    meta.CsvHead["부가정보"],
	IdFieldIdx: 0,
}

var doro = data.FilePhase{
	PrefixName: "개선",
	CsvHead:    meta.CsvHead["개선"],
	IdFieldIdx: 0,
}

var collector data.Collector = &data.FileCollector{
	Src:          pwd + "/juso",
	Dest:         dest,
	FilePhases:   []data.FilePhase{juso, jibun, buga, doro},
	FromEncoding: "CP949",
}

func main() {

	//os.RemoveAll(dest)

	// do collect seperated files and encode them as utf-8
	//collector.Collect()

	numOfShards := 5

	// do sort
	//var wg sync.WaitGroup
	source := dest
	//wg.Add(4)
	jusoSorter := data.NewFileSorter(juso, source, source+"/chunks", numOfShards, 5)
	jibunSorter := data.NewFileSorter(jibun, source, source+"/chunks", numOfShards, 5)
	//bugaSorter := data.NewFileSorter(buga, source, source+"/chunks", numOfShards, 5)
	//doroSorter := data.NewFileSorter(doro, source, source+"/chunks", numOfShards, 5)
	//
	//jusoSorter.OnComplete = func() { wg.Done() }
	//jibunSorter.OnComplete = func() { wg.Done() }
	//bugaSorter.OnComplete = func() { wg.Done() }
	//doroSorter.OnComplete = func() { wg.Done() }
	//
	//go jusoSorter.Sort()
	//go jibunSorter.Sort()
	//go bugaSorter.Sort()
	//go doroSorter.Sort()

	//46

	//wg.Wait()

	jibunSorter.Join(&jusoSorter,
		func(record []string, matchedRecord []string) []string {
			return nil
		},
		func(record []string) []string {
			return nil
		},
	)

	//
	//file, _ := os.OpenFile(pwd + "/result" + "/chunks" + "지번_-1.txt", data.DefaultFileFlag, data.DefaultFileMode)
	//reader := csv.NewReader(file)
	//reader.Comma = '|'
	//
	//records, _ := reader.ReadAll()
	//
	//println(sort.SliceIsSorted(records, func(i, j int) bool {
	//	return strings.Compare(records[i][0], records[j][0]) == 1
	//}))

}
