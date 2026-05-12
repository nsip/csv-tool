package csvtool

import (
	"fmt"
	"strings"

	. "github.com/nsip/go-generics"
	fd "github.com/nsip/gotk/file-dir"
)

// Create : create csv file with input headers
func Create(csvOut string, hdrNames ...string) (string, error) {
	if len(hdrNames) == 0 {
		return "", fmt.Errorf("no headers provided")
	}

	headers := Map(hdrNames, func(i int, e string) string { return CellEsc(e) })
	hdrRow := strings.Join(headers, ",")
	if csvOut != "" {
		fd.MustWriteFile(csvOut, []byte(hdrRow))
	}
	return hdrRow, nil
}

// if cells are more, ignore redundant cells.
// if cells are less, repeat the last cell to fill remainder headers
func AppendOneRowCells(fPath string, validate bool, cells ...string) error {

	headers, _, err := FileInfo(fPath)
	if err != nil {
		return err
	}
	nHdr, nCell := len(headers), len(cells)
	if nHdr == 0 || nCell == 0 {
		return fmt.Errorf("invalid csv for appending or no cells provided")
	}

	if nHdr > nCell {
		cellLast := cells[nCell-1]
		for i := 0; i < nHdr-nCell; i++ {
			cells = append(cells, cellLast)
		}
	} else if nHdr < nCell {
		cells = cells[:nHdr]
	}

	cellsEsc := []string{}
	for _, cell := range cells {
		cellsEsc = append(cellsEsc, CellEsc(cell))
	}
	return AppendRows(fPath, validate, strings.Join(cellsEsc, ","))
}

func AppendOneRowByMap(fPath string, validate bool, mHdrCell map[string]any, defaultValue any) error {

	if defaultValue == nil {
		defaultValue = ""
	}

	headers, _, err := FileInfo(fPath)
	if err != nil {
		return err
	}

	cells := make([]string, len(headers))
	for i, hdr := range headers {
		cell, ok := mHdrCell[hdr]
		if ok && cell == nil {
			cell = ""
		}
		cells[i] = fmt.Sprint(IF(ok, cell, defaultValue))
	}
	return AppendOneRowCells(fPath, validate, cells...)
}

// Append : extend rows, append rows content to csv file
func AppendRows(fPath string, validate bool, rows ...string) error {
	if len(rows) > 0 {
		fd.MustAppendFile(fPath, []byte(strings.Join(rows, "\n")), true)
	}
	if validate {
		if _, _, err := ScanFile(fPath, nil, true, ""); err != nil {
			return err
		}
	}
	return nil
}

// Combine : extend columns, linkHeaders combination must be UNIQUE in csvA & csvB
func Combine(pathA, pathB string, linkHeaders []string, onlyLinkedRow bool, outPath string) error {

	headersA, _, err := FileInfo(pathA)
	if err != nil {
		return err
	}
	if !SupEq(headersA, linkHeaders) {
		return fmt.Errorf("headers of csv-A must have all link-headers")
	}

	headersB, _, err := FileInfo(pathB)
	if err != nil {
		return err
	}
	if !SupEq(headersB, linkHeaders) {
		return fmt.Errorf("headers of csv-B must have all link-headers")
	}

	Create(outPath, Settify(Union(headersA, headersB)...)...)

	var (
		lkIndicesA = Map(linkHeaders, func(i int, e string) int { return IdxOf(e, headersA...) })
		lkIndicesB = Map(linkHeaders, func(i int, e string) int { return IdxOf(e, headersB...) })
		emptyComma = strings.Repeat(",", len(headersB)-len(linkHeaders))
		lkCellsGrp = [][]string{}
		mAiBr      = make(map[int]string)
	)

	_, rowsA, _ := ScanFile(
		pathA,
		func(i, n int, headers, cells []string) (bool, string, string) {

			lkrCells := Map(lkIndicesA, func(i, e int) string { return cells[e] })
			lkCellsGrp = append(lkCellsGrp, lkrCells)

			cells4w := Map(cells, func(i int, e string) string { return CellEsc(e) })
			return true, "", strings.Join(cells4w, ",")
		},
		false,
		"",
	)

	ScanFile(
		pathB,
		func(i, n int, headers, cells []string) (bool, string, string) {
			for iAtRowA, lkrCells := range lkCellsGrp {
				if IsSuper(cells, lkrCells) {
					cells4w := FilterMap(cells,
						func(i int, e string) bool { return NotIn(i, lkIndicesB...) },
						func(i int, e string) string { return CellEsc(e) },
					)
					mAiBr[iAtRowA] = strings.Join(cells4w, ",")
					return false, "", ""
				}
			}
			return false, "", ""
		},
		false,
		"",
	)

	rowsC := []string{}
	if onlyLinkedRow {
		for i, rA := range rowsA {
			if rB, ok := mAiBr[i]; ok {
				rowsC = append(rowsC, rA+","+rB)
			}
		}
	} else {
		for i, rA := range rowsA {
			if rB, ok := mAiBr[i]; ok {
				rowsC = append(rowsC, rA+","+rB)
			} else {
				rowsC = append(rowsC, rA+emptyComma)
			}
		}
	}

	AppendRows(outPath, true, rowsC...)
	return nil
}
