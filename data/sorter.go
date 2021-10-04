package data

import (
	"fmt"
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

func NewFileSorter(dest string, filePhases []FilePhase, numOfShards uint32, numOfCache uint32) FileSorter {
	if numOfShards == 0 || numOfShards > 50 {
		fmt.Printf("numOfShards should be between 10 and 50, but your value was %d", numOfShards)
		numOfShards = 10
	}

	if numOfCache == 0 || numOfCache > 50 {
		fmt.Errorf("numOfCache should be between 10 and 50, but your value was %d", numOfCache)
		numOfCache = 1
	}

	sorter := FileSorter{
		Dest:       dest,
		FilePhases: filePhases,
		shardService: ShardService{
			shardInfo: ShardInfo{
				NumOfShards: numOfShards,
			},
		},
	}

	return sorter
}

func (f FileSorter) Sort(fp FilePhase, idField string) {

	fp.GetFilename()
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
	shardInfo ShardInfo
}

func (ss *ShardService) ParseShardId(rowId uint32) string {
	num := ss.shardInfo.NumOfShards
	return strconv.Itoa(int(rowId % num))
}

type ShardInfo struct {
	NumOfShards uint32
}
