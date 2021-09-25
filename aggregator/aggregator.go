package aggregator

import (
	"os"
	"strings"
)

type Aggregator interface {
	Aggregate()
	Assemble()
}

type FilePhase struct {
	PrefixName string
}

type FileAggregator struct {
	SrcDir     string
	DestDir    string
	FilePhases []FilePhase
}

func (fa *FileAggregator) Aggregate() {
	fa.mergeByPrefix()
}

func (fa *FileAggregator) Assemble() {

}

func (fa *FileAggregator) mergeByPrefix() {
	files, _ := os.ReadDir(fa.SrcDir)

	for _, file := range files {
		if filePhase := fa.getFilePhaseByFileName(file.Name()); filePhase != nil {
			// TODO: encode & append
		}
	}
}

func (fa *FileAggregator) getFilePhaseByFileName(name string) *FilePhase {
	for _, filePhase := range fa.FilePhases {
		if strings.HasPrefix(name, filePhase.PrefixName+"_") {
			return &filePhase
		}
	}

	return nil
}
