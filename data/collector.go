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

var (
	DEFAULT_BUFFER_SIZE = 1024 * 1024
)

type Collector interface {
	Collect()
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

// Collect whole files which belong to the src directory
// example 주소_경기도.txt 주소_강원도.txt -----> 주소.txt
func (fc *FileCollector) Collect() {
	if len(fc.FromEncoding) > 0 {
		fc.merge(createEncodingFilter(fc.FromEncoding))
	} else {
		fc.merge()
	}
}

func (fc *FileCollector) NewReader(filePhase FilePhase) (io.Reader, error) {
	return os.OpenFile(fc.Dest+"/"+filePhase.PrefixName+".txt", DefaultFileFlag, DefaultFileMode)
}

type workableFile struct {
	file      os.DirEntry
	filePhase FilePhase
}

type workableFileGroups map[string][]workableFile

func (fc *FileCollector) merge(filters ...func(bytes []byte) ([]byte, error)) {

	var wg sync.WaitGroup

	workableFileGroups := fc.prepareWorkableGroups()

	wg.Add(len(workableFileGroups))

	for _, workableFiles := range workableFileGroups {
		workableFiles := workableFiles
		go func() {
			for _, f := range workableFiles {
				fc.copyFile(f, filters...)
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func (fc *FileCollector) copyFile(wf workableFile, filters ...func(bytes []byte) ([]byte, error)) {
	file := wf.file
	filePhase := wf.filePhase
	rFile, _ := os.OpenFile(fc.Src+"/"+file.Name(), DefaultFileFlag, DefaultFileMode)

	_ = os.Mkdir(fc.Dest, DefaultFileMode)
	wFile, _ := os.OpenFile(fc.Dest+"/"+filePhase.PrefixName+".txt", DefaultFileFlag, DefaultFileMode)

	defer rFile.Close()
	defer wFile.Close()

	var bytes = make([]byte, DEFAULT_BUFFER_SIZE)

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
}

func (fc *FileCollector) prepareWorkableGroups() workableFileGroups {

	ret := workableFileGroups{}

	files, _ := os.ReadDir(fc.Src)
	for _, file := range files {
		var filePhase *FilePhase
		if file.IsDir() {
			continue
		}
		if filePhase = fc.matchFilePhase(file.Name()); filePhase == nil {
			continue
		}

		workableFiles, isExists := ret[filePhase.PrefixName]

		if !isExists {
			ret[filePhase.PrefixName] = []workableFile{}
			workableFiles = ret[filePhase.PrefixName]
		}

		ret[filePhase.PrefixName] = append(workableFiles, workableFile{file: file, filePhase: *filePhase})
	}

	return ret
}

func (fc *FileCollector) matchFilePhase(filename string) *FilePhase {
	for _, filePhase := range fc.FilePhases {
		if strings.HasPrefix(filename, filePhase.PrefixName+"_") {
			return &filePhase
		}
	}

	return nil
}

func createEncodingFilter(encoding string) func(bytes []byte) ([]byte, error) {
	return func(bytes []byte) ([]byte, error) {
		str, err := iconv.ConvertString(string(bytes), encoding, "utf-8")
		return []byte(str), err
	}
}
