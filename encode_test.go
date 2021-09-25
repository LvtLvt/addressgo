package main

import (
	"github.com/djimenez/iconv-go"
	"os"
	"testing"
)

func TestUtf8Encoding(t *testing.T) {

	readFile, _ := os.OpenFile("./juso/지번_경기도.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)
	os.Mkdir("./result", os.ModePerm)
	writeFile, _ := os.OpenFile("./result/jibun.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)

	defer readFile.Close()
	defer writeFile.Close()

	var bytes = make([]byte, 1024*1000)

	for {
		n, _ := readFile.Read(bytes)

		if n == 0 {
			break
		}

		str, _ := iconv.ConvertString(string(bytes[:n]), "euc-kr", "utf-8")

		writeFile.WriteString(str)
	}
}
