package data

import (
	"os"
	"strconv"
)

type ShardService struct {
	shardInfo         ShardInfo
	fileCashStore     map[string]*os.File
	isFileCashEnabled bool
}

func (ss *ShardService) ParseShardId(rowKey string, metaName string) string {
	num := ss.shardInfo.NumOfShards
	return metaName + "_" + strconv.Itoa(hash(rowKey)%num)
}

func (ss *ShardService) OpenFile(rowKey string, metaName string) *os.File {
	shardId := ss.ParseShardId(rowKey, metaName)

	var file *os.File

	if ss.isFileCashEnabled {
		cashedFile, isExists := ss.fileCashStore[shardId]

		if !isExists {
			cashedFile, _ = os.OpenFile(ss.shardInfo.Dest+"/"+shardId+".txt", DefaultFileFlag, DefaultFileMode)
			ss.fileCashStore[shardId] = cashedFile
		}

		file = cashedFile
	} else {
		// evict cache store
		if len(ss.fileCashStore) > 0 {
			for key, o := range ss.fileCashStore {
				o.Close()
				delete(ss.fileCashStore, key)
			}
		}
		file, _ = os.OpenFile(ss.shardInfo.Dest+"/"+shardId+".txt", DefaultFileFlag, DefaultFileMode)
	}

	return file
}

type ShardInfo struct {
	NumOfShards int
	Dest        string
}
