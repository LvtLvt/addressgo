package data

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Sorter interface {
	Sort()
	Join()
	FindById()
	SetId()
}

type FileSorter struct {
	Source       string
	Dest         string
	FilePhases   []FilePhase
	shardService ShardService
	OnComplete   func()
}

func NewFileSorter(source, dest string, numOfShards int, numOfCache int) FileSorter {
	if numOfShards == 0 || numOfShards > 50 {
		fmt.Printf("numOfShards should be between 1 and 50, but your value was %d", numOfShards)
		numOfShards = 10
	}

	if numOfCache == 0 || numOfCache > 50 {
		fmt.Errorf("numOfCache should be between 1 and 50, but your value was %d", numOfCache)
		numOfCache = 1
	}

	sorter := FileSorter{
		Source: source,
		Dest:   dest,
		shardService: ShardService{
			shardInfo: ShardInfo{
				NumOfShards: numOfShards,
				Dest:        dest,
			},
			fileCashStore: map[string]*os.File{},
		},
	}

	return sorter
}

func (f FileSorter) Sort(fp FilePhase) {
	os.Mkdir(f.Dest, DefaultFileMode)

	rFile, _ := os.OpenFile(f.Source+"/"+fp.GetFilename(), DefaultFileFlag, DefaultFileMode)
	defer rFile.Close()

	reader := csv.NewReader(rFile)
	reader.Comma = '|'
	//reader.ReuseRecord = true

	for {
		record, err := reader.Read()

		if record == nil || err == io.EOF {
			break
		}

		f.doSort(record, fp)
	}

	if f.OnComplete != nil {
		f.OnComplete()
	}
}

func (f FileSorter) doSort(record []string, filePhase FilePhase) {
	id := record[filePhase.IdFieldIdx]
	shardFile := f.shardService.OpenFile(id, filePhase.PrefixName)

	// shard's data
	reader := newCsvReader(shardFile)

	records, _ := reader.ReadAll()
	records = append(records, record)

	rs := &recordSorter{
		idFieldIdx: filePhase.IdFieldIdx,
		records:    &records,
	}

	sort.Sort(rs)

	writer := newCsvWriter(shardFile)

	for _, record := range records {
		writer.Write(record)
	}
	//writer.WriteAll(records)
	writer.Flush()
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
	shardInfo          ShardInfo
	fileCashStoreMutex sync.RWMutex
	fileCashStore      map[string]*os.File
}

func (ss *ShardService) ParseShardId(rowKey string, metaName string) string {
	num := ss.shardInfo.NumOfShards
	return metaName + "_" + strconv.Itoa(hash(rowKey)%num)
}

func (ss *ShardService) OpenFile(rowKey string, metaName string) *os.File {
	shardId := ss.ParseShardId(rowKey, metaName)

	file, isExists := ss.fileCashStore[shardId]

	if !isExists {
		file, _ := os.OpenFile(ss.shardInfo.Dest+"/"+shardId+".txt", DefaultFileFlag, DefaultFileMode)
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
	return strings.Compare(records[i][rs.idFieldIdx], records[j][rs.idFieldIdx]) == -1
}

func hash(key string) int {
	h := 0
	for i := 0; i < len(key); i++ {
		h = 31*h + int(key[i])
	}
	return h << 1
}
