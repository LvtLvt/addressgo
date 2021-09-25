package main

type Aggregator interface {
	Aggregate()
	Assemble()
}

type FilePhases struct {
	PrefixName string
}

type FileAggregator struct {
	FilePhases []FilePhases
}

func (fa *FileAggregator) Aggregate() {
	fa.mergeByPrefix()
}

func (fa *FileAggregator) Assemble() {

}

func (fa *FileAggregator) mergeByPrefix() {

}
