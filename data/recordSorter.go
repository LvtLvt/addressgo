package data

import (
	"sort"
	"strings"
)

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

func (rs recordSorter) Sort() {
	records := *rs.records
	sort.Slice(records, func(i, j int) bool {
		return strings.Compare(records[i][rs.idFieldIdx], records[j][rs.idFieldIdx]) == -1
	})
}

func (rs recordSorter) Search(key string) []string {
	records := *rs.records
	startIndex := 0
	endIndex := len(records) - 1
	midIndex := len(records) / 2
	for startIndex <= endIndex {

		record := records[midIndex]

		if strings.Compare(record[rs.idFieldIdx], key) == 0 {
			return record
		}

		if strings.Compare(record[rs.idFieldIdx], key) == 1 {
			endIndex = midIndex - 1
			midIndex = (startIndex + endIndex) / 2
			continue
		}

		startIndex = midIndex + 1
		midIndex = (startIndex + endIndex) / 2
	}
	return nil
}
