package csvtool

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	. "github.com/digisan/go-generics"
	fd "github.com/digisan/gotk/file-dir"
	lk "github.com/digisan/logkit"
)

func CellEsc(cell string) string {
	if len(cell) > 1 {
		hasComma, hasDQ, hasLF := strings.Contains(cell, ","), strings.Contains(tryStripDQ(cell), "\""), strings.Contains(cell, "\n")
		if hasDQ {
			cell = strings.ReplaceAll(cell, "\"", "\"\"")
		}
		if hasComma || hasLF || hasDQ {
			cell = tryWrapWithDQ(cell)
		}
	}
	return cell
}

// Info : headers, nRow, error
func Info(r io.Reader) ([]string, int, error) {
	rs, ok := r.(io.ReadSeeker)
	if ok {
		defer rs.Seek(0, io.SeekStart)
	}

	content, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, -1, err
	}

	if len(content) == 0 {
		return []string{}, 0, nil
	}
	return content[0], len(content) - 1, nil
}

// CsvInfo : headers, nRow, error
func FileInfo(path string) ([]string, int, error) {
	csvFile, err := os.Open(path)
	if err != nil {
		if csvFile != nil {
			csvFile.Close()
		}
		return nil, 0, err
	}
	defer csvFile.Close()
	return Info(csvFile)
}

func HeaderHasAll(r io.Reader, hdr ...string) (bool, error) {
	headers, _, err := Info(r)
	if err != nil {
		return false, err
	}
	for _, h := range hdr {
		if NotIn(h, headers...) {
			return false, nil
		}
	}
	return true, nil
}

func FileHeaderHasAll(path string, hdr ...string) (bool, error) {
	csvFile, err := os.Open(path)
	if err != nil {
		if csvFile != nil {
			csvFile.Close()
		}
		return false, err
	}
	defer csvFile.Close()
	return HeaderHasAll(csvFile, hdr...)
}

func HeaderHasAny(r io.Reader, hdrs ...string) (bool, error) {
	headers, _, err := Info(r)
	if err != nil {
		return false, err
	}
	for _, hdr := range hdrs {
		if In(hdr, headers...) {
			return true, nil
		}
	}
	return false, nil
}

func FileHeaderHasAny(path string, hdr ...string) (bool, error) {
	csvFile, err := os.Open(path)
	if err != nil {
		if csvFile != nil {
			csvFile.Close()
		}
		return false, err
	}
	defer csvFile.Close()
	return HeaderHasAny(csvFile, hdr...)
}

// CsvReader : streaming row-by-row processing. Malformed rows are skipped with a warning
// rather than aborting and leaving the output empty. n passed to callback is 0 (unknown
// ahead of time); callers must not rely on n being accurate.
func CsvReader(
	r io.Reader,
	f func(i, n int, headers, cells []string) (ok bool, hdr, row string),
	keepOriHdr bool,
	keepAnyRow bool,
	w io.Writer,
) (string, []string, error) {

	rs, ok := r.(io.ReadSeeker)
	if ok {
		defer rs.Seek(0, io.SeekStart)
	}

	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1 // tolerate rows with varying field counts

	// Read header row
	rawHeader, err := reader.Read()
	if err == io.EOF {
		return "", []string{}, fmt.Errorf("FILE_EMPTY")
	}
	if err != nil {
		return "", nil, err
	}

	headers := make([]string, 0, len(rawHeader))
	for i, cell := range rawHeader {
		if cell == "" {
			cell = fmt.Sprintf("column_%d", i)
			lk.WarnOnErr("%v: column[%d] is empty, marked [%s]", fmt.Errorf("CSV_COLUMN_HEADER_EMPTY"), i, cell)
		}
		headers = append(headers, CellEsc(cell))
	}

	hdrLine := strings.Join(headers, ",")
	allRows := []string{}
	// Pre-compute IsNil(w) once, outside the hot row loop.
	// IsNil (from go-generics v0.5.4) falls back to fmt.Sprint(v) which
	// serialises the entire writer struct via reflection on every call.
	// For a *bufio.Writer with a 1 MiB buffer that means ~1 MB of reflect
	// work per row: 2.3 M rows × 1 MB ≈ total freeze.
	// Cache the result here so the loop pays O(1) per row.
	wNotNil := !IsNil(w)

	// If no callback: collect all rows and write them out
	if f == nil {
		rowIdx := 0
		for {
			rawRow, readErr := reader.Read()
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				lk.Warn("CSV parse error at row %d: %v — row skipped", rowIdx+2, readErr)
				rowIdx++
				continue
			}
			allRows = append(allRows, strings.Join(rawRow, ","))
			rowIdx++
		}
		if len(allRows) > 0 || keepOriHdr {
			// hdrLine already set
		} else {
			hdrLine = ""
		}
		goto SAVE
	}

	// Streaming callback processing: write to w as each accepted row arrives.
	// pendingHdr tracks the latest header value returned by callbacks; it is
	// written to w just before the first accepted content row, so that Subset's
	// column-filtered header is captured correctly even if early rows are skipped.
	//
	{
		pendingHdr := hdrLine
		wroteHdr := false
		rowIdx := 0

		for {
			rawRow, readErr := reader.Read()
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				lk.Warn("CSV parse error at row %d: %v — row skipped", rowIdx+2, readErr)
				rowIdx++
				continue
			}

			cbOk, hdr, row := f(rowIdx, 0, headers, rawRow)
			rowIdx++

			if cbOk {
				if hdr != "" {
					pendingHdr = hdr
				}

				if keepAnyRow || !isBlank(row) {
					allRows = append(allRows, row)

					if wNotNil {
						if !wroteHdr {
							if _, werr := fmt.Fprint(w, pendingHdr); werr != nil {
								return "", nil, werr
							}
							wroteHdr = true
						}
						if _, werr := fmt.Fprintf(w, "\n%s", row); werr != nil {
							return "", nil, werr
						}
					}
				}
			}
		}

		// Header-only file (no content rows passed the filter but keepOriHdr requested)
		if wNotNil && !wroteHdr && keepOriHdr {
			if _, werr := fmt.Fprint(w, pendingHdr); werr != nil {
				return "", nil, werr
			}
		}

		hdrLine = pendingHdr
		return hdrLine, allRows, nil
	}

SAVE:
	if wNotNil {
		data := []byte(strings.TrimSuffix(hdrLine+"\n"+strings.Join(allRows, "\n"), "\n"))
		_, err = w.Write(data)
		if err != nil {
			return "", nil, err
		}
	}
	return hdrLine, allRows, nil
}

// Scan : if [f arg: i==-1], it is pure HeaderRow csv
func Scan(in []byte, f func(i, n int, headers, cells []string) (ok bool, hdr, row string), keepOriHdr bool, w io.Writer) (string, []string, error) {
	return CsvReader(bytes.NewReader(in), f, keepOriHdr, false, w)
}

// ScanFile :
func ScanFile(path string, f func(i, n int, headers, cells []string) (ok bool, hdr, row string), keepOriHdr bool, outPath string) (string, []string, error) {

	fr, err := os.Open(path)
	if err != nil {
		return "", nil, err
	}
	defer fr.Close()

	var fw *os.File = nil

	if trimBlank(outPath) != "" {
		fd.MustCreateDir(filepath.Dir(outPath))
		fw, err = os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return "", nil, err
		}
		defer fw.Close()
	}

	hRow, rows, err := CsvReader(fr, f, keepOriHdr, false, fw)
	if rows == nil && err != nil { // go internal csv func error
		return "", nil, err
	}
	return hRow, rows, err
}
