package data

import (
	"encoding/csv"
	"fmt"
	"os"
)

type Index struct {
	Source       string
	Dest         string
	FilePhase    FilePhase
	shardService ShardService
	Records      [][]string
}

func NewFileIndex(filePhase FilePhase, source, dest string, numOfShards int, numOfCache int) Index {
	if numOfShards == 0 || numOfShards > 50 {
		fmt.Printf("numOfShards should be between 1 and 50, but your value was %d", numOfShards)
		numOfShards = 10
	}

	if numOfCache == 0 || numOfCache > 50 {
		fmt.Errorf("numOfCache should be between 1 and 50, but your value was %d", numOfCache)
		numOfCache = 1
	}

	sorter := Index{
		Source: source,
		Dest:   dest,
		shardService: ShardService{
			shardInfo: ShardInfo{
				NumOfShards: numOfShards,
				Dest:        dest,
			},
			fileCashStore:     map[string]*os.File{},
			isFileCashEnabled: true,
		},
		FilePhase: filePhase,
	}

	return sorter
}

func (f *Index) LoadRecords() {
	file := f.shardService.OpenFile("0", f.FilePhase.PrefixName)
	reader := newCsvReader(file)
	records, _ := reader.ReadAll()

	f.Records = records
}

func (f *Index) ClearRecords() {
	f.Records = [][]string{}
}

func (f *Index) Sort(onComplete func()) {
	os.Mkdir(f.Dest, DefaultFileMode)

	rFile, _ := os.OpenFile(f.Source+"/"+f.FilePhase.GetFilename(), DefaultFileFlag, DefaultFileMode)
	defer rFile.Close()

	reader := csv.NewReader(rFile)
	reader.Comma = '|'

	records, _ := reader.ReadAll()
	wFile := f.shardService.OpenFile("0", f.FilePhase.PrefixName)
	writer := newCsvWriter(wFile)
	writer.WriteAll(records)

	f.doSort()

	if onComplete != nil {
		onComplete()
		f.shardService.fileCashStore = map[string]*os.File{}
	}
}

func (f *Index) Join(
	targetSorter *Index,
	onMatch func(record []string, matchedRecord []string) []string,
	onNotFound func(record []string) []string,
) {
	// turn off cashStore to avoid concurrent access conflicts
	//if targetSorter.shardService.isFileCashEnabled {
	//	targetSorter.shardService.isFileCashEnabled = false
	//
	//	defer func() {targetSorter.shardService.isFileCashEnabled = true}()
	//}
	targetSorter.LoadRecords()
	chunkFiles, isExist := groupFilesByPrefix(f.Dest, f.FilePhase)[f.FilePhase.PrefixName]

	if !isExist {
		return
	}

	for _, fileInfo := range chunkFiles {
		func() {
			file, _ := os.OpenFile(f.Dest+"/"+fileInfo.file.Name(), DefaultFileFlag, DefaultFileMode)
			defer file.Close()

			reader := newCsvReader(file)
			writer := newCsvWriter(file)
			records, _ := reader.ReadAll()

			// inner join
			for i, record := range records {
				targetRow := targetSorter.FindById(record[f.FilePhase.IdFieldIdx])
				if targetRow != nil {
					records[i] = onMatch(record, targetRow)
				} else {
					records[i] = onNotFound(record)
				}
			}

			file.Truncate(0)
			writer.WriteAll(records)
		}()
	}
	targetSorter.ClearRecords()
}

func (f *Index) FindById(key string) []string {
	//file := f.shardService.OpenFile(key, f.FilePhase.PrefixName)
	//reader := newCsvReader(file)
	//Records, _ := reader.ReadAll()

	records := f.Records

	sorter := recordSorter{
		records:    &records,
		idFieldIdx: f.FilePhase.IdFieldIdx,
	}

	return sorter.Search(key)
}

func (f *Index) bucketize(record []string, filePhase FilePhase) {
	id := record[filePhase.IdFieldIdx]
	shardFile := f.shardService.OpenFile(id, filePhase.PrefixName)
	//defer shardFile.Close()

	// shard's data
	writer := newCsvWriter(shardFile)
	writer.Write(record)
	writer.Flush()
}

func (f *Index) doSort() {

	files, isExist := groupFilesByPrefix(f.Dest, f.FilePhase)[f.FilePhase.PrefixName]

	if !isExist {
		return
	}

	for _, fileInfo := range files {
		func() {
			file, _ := os.OpenFile(f.Dest+"/"+fileInfo.file.Name(), DefaultFileFlag, DefaultFileMode)
			defer file.Close()

			reader := newCsvReader(file)
			records, _ := reader.ReadAll()

			rs := &recordSorter{
				idFieldIdx: f.FilePhase.IdFieldIdx,
				records:    &records,
			}

			rs.Sort()

			file.Truncate(0)
			writer := newCsvWriter(file)
			writer.WriteAll(records)
		}()
	}
}

func (f *Index) getFileFullName(filePhase FilePhase) string {
	return f.Dest + "/" + filePhase.GetFilename()
}

func newCsvReader(f *os.File) csv.Reader {
	reader := csv.NewReader(f)
	reader.Comma = '|'
	return *reader
}

func newCsvWriter(f *os.File) *csv.Writer {
	writer := csv.NewWriter(f)
	writer.Comma = '|'
	return writer
}

func hash(key string) int {
	h := 0
	for i := 0; i < len(key); i++ {
		h = 31*h + int(key[i])
	}
	return h
}
