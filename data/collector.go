package data

import (
	"fmt"
	"github.com/djimenez/iconv-go"
	"io"
	"os"
	"strings"
	"sync"
)

const (
	DefaultFileFlag = os.O_RDWR | os.O_CREATE | os.O_APPEND
	DefaultFileMode = os.ModePerm
)

type Collector interface {
	Aggregate()
	NewReader(filePhase FilePhase) (io.Reader, error)
}

type FilePhase struct {
	PrefixName string
	CsvHead    []string
}

func (fp *FilePhase) GetCsvHeadStr() string {
	return strings.Join(fp.CsvHead, "|") + "\n"
}

func (fp *FilePhase) GetFilename() string {
	return fp.PrefixName + ".txt"
}

type FileCollector struct {
	Src          string
	Dest         string
	FilePhases   []FilePhase
	FromEncoding string
	pwd          string
}

// Aggregate whole files which belong to the src directory
// example 주소_경기도.txt 주소_강원도.txt -----> 주소.txt
//
func (fa *FileCollector) Aggregate() {
	if len(fa.FromEncoding) > 0 {
		fa.merge(makeEncodingFilter(fa.FromEncoding))
	} else {
		fa.merge()
	}
}

func (fa *FileCollector) NewReader(filePhase FilePhase) (io.Reader, error) {
	return os.OpenFile(fa.Dest+"/"+filePhase.PrefixName+".txt", DefaultFileFlag, DefaultFileMode)
}

type workableFileGroups struct {
	file      os.DirEntry
	filePhase FilePhase
}

func (fa *FileCollector) merge(filters ...func(bytes []byte) ([]byte, error)) {

	var wg sync.WaitGroup

	workableFileGroups := fa.prepareWorkableGroups()

	wg.Add(len(workableFileGroups))

	for _, workableFile := range workableFileGroups {
		file := workableFile.file
		filePhase := workableFile.filePhase
		go func() {
			rFile, _ := os.OpenFile(fa.Src+"/"+file.Name(), DefaultFileFlag, DefaultFileMode)
			rFileState, _ := rFile.Stat()

			_ = os.Mkdir(fa.Dest, DefaultFileMode)
			wFile, _ := os.OpenFile(fa.Dest+"/"+filePhase.PrefixName+".txt", DefaultFileFlag, DefaultFileMode)

			defer rFile.Close()
			defer wFile.Close()
			defer wg.Done()

			var bytes = make([]byte, rFileState.Size())

			// TODO: move to the place where before execution
			fileInfo, _ := wFile.Stat()
			if fileInfo.Size() == 0 {
				wFile.WriteString(filePhase.GetCsvHeadStr())
			}

			for {
				n, err := rFile.Read(bytes)

				if err != nil {
					if err == io.EOF {
						break
					} else {
						_ = fmt.Errorf("while reading, error occured cause: %s", err.Error())
						os.Exit(-1)
					}
				}

				var out = bytes[:n]

				// do filter
				for _, filter := range filters {
					out, _ = filter(out)
				}

				if err != nil {
					_ = fmt.Errorf("while converting, error occured cause: %s", err.Error())
					os.Exit(-1)
				}

				_, err = wFile.Write(out)

				if err != nil {
					_ = fmt.Errorf("while writing, error occured cause: %s", err.Error())
					os.Exit(-1)
				}
			}
		}()
	}

	wg.Wait()
}

func (fa *FileCollector) prepareWorkableGroups() []workableFileGroups {

	var ret []workableFileGroups

	files, _ := os.ReadDir(fa.Src)
	for _, file := range files {
		var filePhase *FilePhase
		if file.IsDir() {
			continue
		}
		if filePhase = fa.matchFilePhase(file.Name()); filePhase == nil {
			continue
		}

		ret = append(ret, workableFileGroups{file: file, filePhase: *filePhase})
	}

	return ret
}

func (fa *FileCollector) matchFilePhase(filename string) *FilePhase {
	for _, filePhase := range fa.FilePhases {
		if strings.HasPrefix(filename, filePhase.PrefixName+"_") {
			return &filePhase
		}
	}

	return nil
}

func makeEncodingFilter(encoding string) func(bytes []byte) ([]byte, error) {
	switch strings.ToUpper(encoding) {
	case "EUC-KR":
		return func(bytes []byte) ([]byte, error) {
			str, err := iconv.ConvertString(string(bytes), encoding, "utf-8")
			return []byte(str), err
		}
	default:
		panic("unsupported encoding")
	}
}
