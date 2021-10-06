package main

import (
	"context"
	"csvgo/data"
	"csvgo/data/meta"
	"encoding/csv"
	"github.com/olivere/elastic"
	"os"
)

type address struct {
	관리번호   string
	일련번호   string
	법정동코드  string
	시도명    string
	시군구명   string
	법정읍면동명 string
	법정리명   string
	산여부    string
	지번본번   string
	지번부번   string
	대표여부   string

	// mixin
	건물본번   string
	건물부번   string
	읍면동명   string
	도로명코드  string
	지번풀네임  string
	도로명풀네임 string
}

func main2() {
	wd, _ := os.Getwd()
	file, _ := os.OpenFile(wd+"/result/chunks/지번_0.txt", data.DefaultFileFlag, data.DefaultFileMode)

	reader := csv.NewReader(file)

	records, _ := reader.ReadAll()

	client, err := elastic.NewClient(
		elastic.SetURL("http://localhost:9200"),
	)

	println(err)

	ctx := context.Background()

	bk := client.Bulk()

	for i, record := range records {
		if i == 50000 {
			break
		}

		req := elastic.NewBulkIndexRequest()
		req.OpType("index")
		req.Index("real_addres")
		req.Id(string(rune(i)))
		req.Doc(address{
			관리번호:   record[meta.Jibun_관리번호],
			일련번호:   record[meta.Jibun_일련번호],
			법정동코드:  record[meta.Jibun_법정동코드],
			시도명:    record[meta.Jibun_시도명],
			시군구명:   record[meta.JIbun_시군구명],
			법정읍면동명: record[meta.Jibun_법정읍면동명],
			법정리명:   record[meta.JIbun_법정리명],
			산여부:    record[meta.Jibun_산여부],
			지번본번:   record[meta.Jibun_지번본번],
			지번부번:   record[meta.Jibun_지번부번],
			대표여부:   record[meta.Jibun_대표여부],
			건물본번:   record[meta.Jibun_건물본번],
			건물부번:   record[meta.Jibun_건물부번],
			읍면동명:   record[meta.Jibun_읍면동명],
			도로명코드:  record[meta.Jibun_도로명코드],
			지번풀네임:  record[meta.Jibun_지번풀네임],
			도로명풀네임: record[meta.Jibun_도로명풀네임],
		})
		bk.Add(req)
	}

	bk.Do(ctx)
}
