package main

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"

	"github.com/dsnet/compress/bzip2"
	lru "github.com/hashicorp/golang-lru"
	"github.com/lib/pq"
)

var fqdnCache *lru.Cache

func initFQDNLRUCache(size int) (*lru.Cache, error) {
	return lru.New(size)
}

func pqStringArray(ss []string) interface{} {
	if ss == nil {
		return nil
	}
	return pq.Array(ss)
}

func getReader(archivePath string, useGzip, useBzip2 bool) (*bufio.Reader, error) {
	var r io.Reader
	if archivePath == "" || archivePath == "-" {
		r = os.Stdin
	} else {
		file, err := os.Open(archivePath)
		if err != nil {
			return nil, err
		}
		r = file
	}
	if useGzip {
		gr, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		return bufio.NewReader(gr), nil
	}
	if useBzip2 {
		br, err := bzip2.NewReader(r, nil)
		if err != nil {
			return nil, err
		}
		return bufio.NewReader(br), nil
	}
	return bufio.NewReader(r), nil
}
