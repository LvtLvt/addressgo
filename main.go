package main

import (
	"csvgo/data"
	"csvgo/data/meta"
	"os"
	"strings"
	"sync"
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

	os.RemoveAll(dest)

	// do collect seperated files and encode them as utf-8
	collector.Collect()

	numOfShards := 5

	// do sort
	var wg sync.WaitGroup
	source := dest
	wg.Add(4)
	jusoIndex := data.NewFileIndex(juso, source, source+"/chunks", numOfShards, 5)
	jibunIndex := data.NewFileIndex(jibun, source, source+"/chunks", numOfShards, 5)
	bugaIndex := data.NewFileIndex(buga, source, source+"/chunks", numOfShards, 5)
	doroIndex := data.NewFileIndex(doro, source, source+"/chunks", numOfShards, 5)

	go jusoIndex.Sort(func() { wg.Done() })
	go jibunIndex.Sort(func() { wg.Done() })
	go bugaIndex.Sort(func() { wg.Done() })
	go doroIndex.Sort(func() { wg.Done() })

	wg.Wait()
	defaultFieldValue := "0"
	jusoIndex.FilePhase.IdFieldIdx = 1
	jusoIndex.Join(&doroIndex,
		func(record []string, matchedRecord []string) []string {
			return append(record, matchedRecord[meta.Doro_읍면동명], matchedRecord[meta.Doro_도로명])
		},
		func(record []string) []string {
			return append(record, defaultFieldValue, defaultFieldValue)
		},
	)

	jusoIndex.FilePhase.IdFieldIdx = 0
	jibunIndex.Join(&jusoIndex,
		func(record []string, matchedRecord []string) []string {
			// build jibun full name
			buffer := strings.Builder{}
			buffer.WriteString(record[meta.Jibun_시도명])
			buffer.WriteString(" ")
			buffer.WriteString(record[meta.JIbun_시군구명])
			if len(record[meta.Jibun_법정읍면동명]) != 0 {
				buffer.WriteString(" ")
				buffer.WriteString(record[meta.Jibun_법정읍면동명])
			}
			if len(record[meta.JIbun_법정리명]) != 0 {
				buffer.WriteString(" ")
				buffer.WriteString(record[meta.JIbun_법정리명])
			}

			if field := record[meta.Jibun_지번본번]; len(field) != 0 {
				buffer.WriteString(" ")
				buffer.WriteString(field)
			}

			if field := record[meta.Jibun_지번부번]; len(field) != 0 {
				buffer.WriteString("-")
				buffer.WriteString(field)
			}

			jibunFullName := buffer.String()
			buffer.Reset()

			// build doro full name
			buffer.WriteString(record[meta.Jibun_시도명])
			buffer.WriteString(" ")
			buffer.WriteString(record[meta.JIbun_시군구명])
			buffer.WriteString(" ")
			buffer.WriteString(matchedRecord[meta.Juso_도로명])
			if field := matchedRecord[meta.Juso_건물본번]; len(field) != 0 && field != defaultFieldValue {
				buffer.WriteString(" ")
				buffer.WriteString(field)
			}

			if field := matchedRecord[meta.Juso_건물부번]; len(field) != 0 && field != defaultFieldValue {
				buffer.WriteString("-")
				buffer.WriteString(field)
			}

			bugaRecord := bugaIndex.FindById(record[0])
			if bugaRecord != nil {
				if field := bugaRecord[meta.Buga_건축물대장건물명]; len(field) != 0 {
					buffer.WriteString(" ")
					buffer.WriteString(field)
				} else if field = bugaRecord[meta.Buga_시군구건물명]; len(field) != 0 {
					buffer.WriteString(" ")
					buffer.WriteString(field)
				}
			}

			doroFullName := buffer.String()
			buffer.Reset()

			return append(record,
				matchedRecord[meta.Juso_건물본번],
				matchedRecord[meta.Juso_건물부번],
				matchedRecord[meta.Juso_읍면동명],
				matchedRecord[meta.Juso_도로명코드],
				jibunFullName,
				doroFullName,
			)
		},
		func(record []string) []string {
			return append(
				record,
				defaultFieldValue,
				defaultFieldValue,
				defaultFieldValue,
				defaultFieldValue,
				defaultFieldValue,
				defaultFieldValue,
			)
		},
	)
}
