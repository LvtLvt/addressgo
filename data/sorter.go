package data

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
)

type Sorter interface {
	Sort()
	Join()
	FindById()
	SetId()
}

type FileSorter struct {
	Dest         string
	FilePhases   []FilePhase
	shardService ShardService
}

func NewFileSorter(dest string, numOfShards int, numOfCache int) FileSorter {
	if numOfShards == 0 || numOfShards > 50 {
		fmt.Printf("numOfShards should be between 10 and 50, but your value was %d", numOfShards)
		numOfShards = 10
	}

	if numOfCache == 0 || numOfCache > 50 {
		fmt.Errorf("numOfCache should be between 10 and 50, but your value was %d", numOfCache)
		numOfCache = 1
	}

	sorter := FileSorter{
		Dest: dest,
		shardService: ShardService{
			shardInfo: ShardInfo{
				NumOfShards: numOfShards,
				Dest:        dest,
			},
		},
	}

	return sorter
}

func (f FileSorter) Sort(fp FilePhase) {
	fileName := f.getFileFullName(fp)

	rFile, _ := os.OpenFile(fileName, DefaultFileFlag, DefaultFileMode)
	defer rFile.Close()

	reader := csv.NewReader(rFile)
	reader.Comma = '|'
	reader.ReuseRecord = true

	for {
		record, err := reader.Read()

		if record == nil || err == io.EOF {
			return
		}

		f.doSort(record, fp)
	}
}

func (f FileSorter) doSort(record []string, filePhase FilePhase) {
	id, _ := strconv.Atoi(record[filePhase.idFieldIdx])
	shardFile := f.shardService.OpenFile(id, filePhase.PrefixName)

	// shard's data
	reader := newCsvReader(shardFile)

	records, _ := reader.ReadAll()
	rs := &recordSorter{
		idFieldIdx: filePhase.idFieldIdx,
		records:    &records,
	}

	sort.Sort(rs)
}

func (f FileSorter) getFileFullName(filePhase FilePhase) string {
	return f.Dest + "/" + filePhase.GetFilename()
}

func (f FileSorter) Join() {
	panic("implement me")
}

func (f FileSorter) FindById() {
	panic("implement me")
}

func (f FileSorter) SetId() {
	panic("implement me")
}

type ShardService struct {
	shardInfo     ShardInfo
	fileCashStore map[string]*os.File
}

func (ss *ShardService) ParseShardId(rowKey int, metaName string) string {
	num := ss.shardInfo.NumOfShards
	return metaName + "_" + strconv.Itoa(rowKey%num)
}

func (ss *ShardService) OpenFile(rowKey int, metaName string) *os.File {
	shardId := ss.ParseShardId(rowKey, metaName)

	file, isExists := ss.fileCashStore[shardId]

	if !isExists {
		file, _ = os.OpenFile(ss.shardInfo.Dest+"/"+shardId, DefaultFileFlag, DefaultFileMode)
		ss.fileCashStore[shardId] = file
	}

	return file
}

type ShardInfo struct {
	NumOfShards int
	Dest        string
}

func newCsvReader(f *os.File) *csv.Reader {
	reader := csv.NewReader(f)
	reader.Comma = '|'
	return reader
}

func newCsvWriter(f *os.File) *csv.Writer {
	writer := csv.NewWriter(f)
	writer.Comma = '|'
	return writer
}

type recordSorter struct {
	records    *[][]string
	idFieldIdx int
}

func (rs recordSorter) Len() int {
	return len(*rs.records)
}

func (rs recordSorter) Swap(i, j int) {
	records := *rs.records
	records[i], records[j] = records[j], records[i]
}

func (rs recordSorter) Less(i, j int) bool {
	records := *rs.records
	left, _ := strconv.Atoi(records[i][rs.idFieldIdx])
	right, _ := strconv.Atoi(records[j][rs.idFieldIdx])
	return left < right
}
