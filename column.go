package csvtool

import (
	"fmt"
	"io"
	"os"

	. "github.com/nsip/go-generics"
)

// ColAttr :
type ColAttr struct {
	Idx       int
	Header    string
	IsEmpty   bool
	IsUnique  bool
	HasNull   bool
	HasEmpty  bool
	AllFilled bool // no cell is "null/NULL/nil" AND no empty cell
}

// Column : header, cells, err
func Column(r io.Reader, idx int) (hdr string, cells []string, err error) {
	rs, ok := r.(io.ReadSeeker)
	if ok {
		defer rs.Seek(0, io.SeekStart)
	}

	headers, _, err := Info(r)
	if err != nil {
		return "", nil, err
	}

	if idx >= len(headers) {
		return "", nil, fmt.Errorf("idx(%d) is out of index range", idx)
	}

	return CsvReader(r, func(i, n int, headers, cells []string) (ok bool, hdr, row string) {
		return true, headers[idx], cells[idx]
	}, true, true, nil)
}

// FileColumn : header, cells, err
func FileColumn(path string, idx int) (hdr string, cells []string, err error) {
	csv, err := os.Open(path)
	if err != nil {
		if csv != nil {
			csv.Close()
		}
		return "", nil, err
	}
	defer csv.Close()
	return Column(csv, idx)
}

// GetColAttr :
func GetColAttr(r io.Reader, idx int) (*ColAttr, error) {
	rs, ok := r.(io.ReadSeeker)
	if ok {
		defer rs.Seek(0, io.SeekStart)
	}

	hdr, cells, err := Column(r, idx)
	if err != nil {
		return nil, err
	}

	ca := &ColAttr{
		Idx:       idx,
		Header:    hdr,
		IsEmpty:   len(cells) == 0,
		IsUnique:  len(cells) == len(Settify(cells...)),
		HasNull:   false,
		HasEmpty:  false,
		AllFilled: true,
	}
	for _, cell := range cells {
		switch trimBlank(cell) {
		case "null", "nil", "NULL":
			ca.HasNull = true
		case "":
			ca.HasEmpty = true
		}
		if ca.HasNull && ca.HasEmpty {
			break
		}
	}
	ca.AllFilled = !ca.HasNull && !ca.HasEmpty
	return ca, nil
}

// FileColAttr :
func FileColAttr(path string, idx int) (*ColAttr, error) {
	csv, err := os.Open(path)
	if err != nil {
		if csv != nil {
			csv.Close()
		}
		return nil, err
	}
	defer csv.Close()
	return GetColAttr(csv, idx)
}
