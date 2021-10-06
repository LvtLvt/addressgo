package data

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

type Entity struct {
	Source       string
	Dest         string
	FilePhase    FilePhase
	shardService ShardService
}

func NewFileIndex(filePhase FilePhase, source, dest string, numOfShards int, numOfCache int) Entity {
	if numOfShards == 0 || numOfShards > 50 {
		fmt.Printf("numOfShards should be between 1 and 50, but your value was %d", numOfShards)
		numOfShards = 10
	}

	if numOfCache == 0 || numOfCache > 50 {
		fmt.Errorf("numOfCache should be between 1 and 50, but your value was %d", numOfCache)
		numOfCache = 1
	}

	sorter := Entity{
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

func (f Entity) Sort(onComplete func()) {
	os.Mkdir(f.Dest, DefaultFileMode)

	rFile, _ := os.OpenFile(f.Source+"/"+f.FilePhase.GetFilename(), DefaultFileFlag, DefaultFileMode)
	defer rFile.Close()

	reader := csv.NewReader(rFile)
	reader.Comma = '|'

	for {
		record, err := reader.Read()

		if record == nil || err == io.EOF {
			break
		}

		f.bucketize(record, f.FilePhase)
	}

	f.doSort()

	if onComplete != nil {
		onComplete()
		f.shardService.fileCashStore = map[string]*os.File{}
	}
}

func (f Entity) Join(
	targetSorter *Entity,
	onMatch func(record []string, matchedRecord []string) []string,
	onNotFound func(record []string) []string,
) {
	// turn off cashStore to avoid concurrent access conflicts
	//if targetSorter.shardService.isFileCashEnabled {
	//	targetSorter.shardService.isFileCashEnabled = false
	//
	//	defer func() {targetSorter.shardService.isFileCashEnabled = true}()
	//}

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
}

func (f Entity) FindById(key string) []string {
	file := f.shardService.OpenFile(key, f.FilePhase.PrefixName)
	reader := newCsvReader(file)
	records, _ := reader.ReadAll()

	sorter := recordSorter{
		records:    &records,
		idFieldIdx: f.FilePhase.IdFieldIdx,
	}

	return sorter.Search(key)
}

func (f Entity) bucketize(record []string, filePhase FilePhase) {
	id := record[filePhase.IdFieldIdx]
	shardFile := f.shardService.OpenFile(id, filePhase.PrefixName)
	defer shardFile.Close()

	// shard's data
	writer := newCsvWriter(shardFile)
	writer.Write(record)
	writer.Flush()
}

func (f Entity) doSort() {

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

func (f Entity) getFileFullName(filePhase FilePhase) string {
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
