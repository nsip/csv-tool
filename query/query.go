package query

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	. "github.com/digisan/go-generics"
	fd "github.com/digisan/gotk/file-dir"
	ct "github.com/nsip/csv-tool"
)

// GetRepeated : remove repeated cells
func GetRepeated(csv, out string, f func(rRepCnt int) bool) (string, []string, error) {
	_, _, mHashCnt, err := Unique(csv, "")
	if err != nil {
		return "", nil, err
	}
	return ct.ScanFile(csv,
		func(i, n int, headers, cells []string) (ok bool, hdr string, row string) {
			md5s := Map(cells, func(i int, e string) string { return fmt.Sprint(md5.Sum([]byte(e))) })
			rowHash := strings.Join(md5s, ",")
			headers4w := Map(headers, func(i int, e string) string { return ct.CellEsc(e) })
			cells4w := Map(cells, func(i int, e string) string { return ct.CellEsc(e) })
			return f(mHashCnt[rowHash]), strings.Join(headers4w, ","), strings.Join(cells4w, ",")
		},
		true,
		out,
	)
}

// Unique : remove repeated cells
func Unique(csv, out string) (string, []string, map[string]int, error) {
	// check out csv file is valid
	defer func() {
		if out != "" {
			ct.ScanFile(out, nil, true, "")
		}
	}()

	mHashCnt := make(map[string]int)
	h, rs, err := ct.ScanFile(
		csv,
		func(idx, cnt int, headers, cells []string) (bool, string, string) {
			md5s := Map(cells, func(i int, e string) string { return fmt.Sprint(md5.Sum([]byte(e))) })
			rowHash := strings.Join(md5s, ",")
			_, ok := mHashCnt[rowHash]
			defer func() { mHashCnt[rowHash]++ }()

			if ok {
				return false, "", ""
			}

			headers4w := Map(headers, func(i int, e string) string { return ct.CellEsc(e) })
			cells4w := Map(cells, func(i int, e string) string { return ct.CellEsc(e) })
			return !ok, strings.Join(headers4w, ","), strings.Join(cells4w, ",")
		},
		true,
		out,
	)
	return h, rs, mHashCnt, err
}

// Subset : content iRow start from 0. i.e. 1st content row index is 0
func Subset(in []byte, incCol bool, hdrNames []string, incRow bool, iRows []int, w io.Writer) (string, []string, error) {

	fnRow := NotIn[int]
	if incRow {
		fnRow = In[int]
	}

	cIndices, hdrRow := []int{}, ""
	fast, min, max := IsContinuous(iRows...)

	return ct.Scan(in, func(idx, cnt int, headers, cells []string) (bool, string, string) {

		// get [hdrRow], [cIndices] once
		if hdrRow == "" {
			// select needed columns, cIndices is qualified header's original index in file headers
			var hdrRt []string
			if incCol {
				cIndices = FilterMap(hdrNames,
					func(i int, e string) bool { return In(e, headers...) },
					func(i int, e string) int { return IdxOf(e, headers...) },
				)
				hdrRt = Reorder(headers, cIndices) // Reorder has filter
				hdrRt = Map(hdrRt, func(i int, e string) string { return ct.CellEsc(e) })
			} else {
				cIndices = FilterMap(headers,
					func(i int, e string) bool { return NotIn(e, hdrNames...) },
					func(i int, e string) int { return i },
				)
				hdrRt = FilterMap(headers,
					func(i int, e string) bool { return In(i, cIndices...) },
					func(i int, e string) string { return ct.CellEsc(e) },
				)
			}
			hdrRow = strings.Join(hdrRt, ",")
		}

		ok := false
		if fast {
			if (incRow && idx >= min && idx <= max) || (!incRow && (idx < min || idx > max)) {
				ok = true
			}
		} else {
			if fnRow(idx, iRows...) {
				ok = true
			}
		}

		if ok {
			// filter column cells
			var cellsRt []string
			if incCol {
				cellsRt = Reorder(cells, cIndices)
				cellsRt = Map(cellsRt, func(i int, e string) string { return ct.CellEsc(e) })
			} else {
				cellsRt = FilterMap(cells,
					func(i int, e string) bool { return In(i, cIndices...) },
					func(i int, e string) string { return ct.CellEsc(e) },
				)
			}

			return true, hdrRow, strings.Join(cellsRt, ",")
		}

		return true, hdrRow, "" // still "ok" as hdrRow is needed even if empty content

	}, true, w)
}

// Cond :
type Cond struct {
	Hdr string
	Val any
	Rel string
}

// Select : R : [&, |]; condition relation : [=, !=, >, <, >=, <=]
// [=, !=] only apply to string comparison, [>, <, >=, <=] apply to number comparison
func Select(in []byte, R rune, CGrp []Cond, w io.Writer) (string, []string, error) {

	if NotIn(R, '&', '|') {
		return "", nil, fmt.Errorf("[R] can only be [&, |]")
	}

	nCGrp := len(CGrp)
	return ct.Scan(in, func(idx, cnt int, headers, cells []string) (bool, string, string) {

		hdrNames := Map(headers, func(i int, e string) string { return ct.CellEsc(e) })
		hdrRow := strings.Join(hdrNames, ",")

		if len(cells) == 0 {
			return true, hdrRow, ""
		}

		CResults := []any{}

	NEXT_CONDITION:
		for _, C := range CGrp {

			if R == '|' && len(CResults) > 0 {
				break NEXT_CONDITION
			}

			if I := IdxOf(C.Hdr, headers...); I != -1 {
				iVal := cells[I]

				if C.Rel == "=" {
					if iVal == C.Val {
						CResults = append(CResults, struct{}{})
					}
					continue NEXT_CONDITION
				}
				if C.Rel == "!=" {
					if iVal != C.Val {
						CResults = append(CResults, struct{}{})
					}
					continue NEXT_CONDITION
				}

				switch Typ := fmt.Sprintf("%T", C.Val); Typ {
				case "int", "int8", "int16", "int32", "int64":
					var cValue int64
					if i64Val, ok := C.Val.(int64); ok {
						cValue = i64Val
					} else if intVal, ok := C.Val.(int); ok {
						cValue = int64(intVal)
					}

					iValue, err := strconv.ParseInt(iVal, 10, 64)
					if err != nil {
						break
					}
					if (C.Rel == ">" && iValue > cValue) ||
						(C.Rel == ">=" && iValue >= cValue) ||
						(C.Rel == "<" && iValue < cValue) ||
						(C.Rel == "<=" && iValue <= cValue) {
						CResults = append(CResults, struct{}{})
					}

				case "uint", "uint8", "uint16", "uint32", "uint64":
					var cValue uint64
					if i64Val, ok := C.Val.(int64); ok {
						cValue = uint64(i64Val)
					} else if intVal, ok := C.Val.(int); ok {
						cValue = uint64(intVal)
					}

					iValue, err := strconv.ParseUint(iVal, 10, 64)
					if err != nil {
						break
					}
					if (C.Rel == ">" && iValue > cValue) ||
						(C.Rel == ">=" && iValue >= cValue) ||
						(C.Rel == "<" && iValue < cValue) ||
						(C.Rel == "<=" && iValue <= cValue) {
						CResults = append(CResults, struct{}{})
					}

				case "float32", "float64", "float", "double":
					cValue := C.Val.(float64)
					iValue, err := strconv.ParseFloat(iVal, 64)
					if err != nil {
						break
					}
					if (C.Rel == ">" && iValue > cValue) ||
						(C.Rel == ">=" && iValue >= cValue) ||
						(C.Rel == "<" && iValue < cValue) ||
						(C.Rel == "<=" && iValue <= cValue) {
						CResults = append(CResults, struct{}{})
					}

				default:
					panic("comparable type [" + Typ + "] is not supported")
				}
			}
		}

		ok := false

		// Has conditions
		if len(CGrp) > 0 {
			if len(CResults) == 0 {
				return true, hdrRow, ""
			}
			if (R == '&' && len(CResults) == nCGrp) || (R == '|' && len(CResults) > 0) {
				ok = true
			}
		}

		// No conditions OR condition ok
		if ok || len(CGrp) == 0 {
			cellValues := Map(cells, func(i int, e string) string { return ct.CellEsc(e) })
			return true, hdrRow, strings.Join(cellValues, ",")
		}

		return true, hdrRow, ""

	}, true, w)
}

// Query : combine Subset(incCol, all rows) & Select
func Query(in []byte, incCol bool, hdrNames []string, R rune, CGrp []Cond, w io.Writer) (string, []string, error) {

	b := &bytes.Buffer{}
	_, _, err := Select(in, R, CGrp, io.Writer(b))
	if err == nil {
		return Subset(b.Bytes(), incCol, hdrNames, false, []int{}, w)
	}
	return "", nil, err

}

func QueryFile(csvPath string, incCol bool, hdrNames []string, R rune, CGrp []Cond, out string) error {

	if !fd.FileExists(csvPath) {
		return fmt.Errorf("[%s] does NOT exist, ignore", csvPath)
	}

	// When csvPath == out we write to a temp file and rename only on success,
	// so a query error never destroys the original.
	actualOut := out
	usingTemp := csvPath == out
	if usingTemp {
		actualOut = out + ".querytmp"
	}

	fr, err := os.Open(csvPath)
	if err != nil {
		return err
	}
	defer fr.Close()

	fd.MustCreateDir(filepath.Dir(actualOut))

	// O_TRUNC ensures a pre-existing file is cleared before writing, preventing
	// stale tail bytes when the new content is shorter than the old content.
	fw, err := os.OpenFile(actualOut, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	in, readErr := io.ReadAll(fr)
	if readErr != nil {
		fw.Close()
		os.Remove(actualOut)
		return readErr
	}

	_, _, queryErr := Query(in, incCol, hdrNames, R, CGrp, fw)
	fw.Close() // close before rename

	if queryErr != nil {
		os.Remove(actualOut) // discard empty/partial output, leave original intact
		return queryErr
	}

	if usingTemp {
		if removeErr := os.Remove(out); removeErr != nil {
			os.Remove(actualOut)
			return fmt.Errorf("QueryFile: could not remove original [%s]: %w", out, removeErr)
		}
		if renameErr := os.Rename(actualOut, out); renameErr != nil {
			return fmt.Errorf("QueryFile: could not rename temp to [%s]: %w", out, renameErr)
		}
	}

	return nil
}

// QueryByConfig :
func QueryByConfig(tomlPath string) (int, error) {

	config := &Config{}
	if _, err := toml.DecodeFile(tomlPath, config); err != nil {
		return 0, err
	}

	for _, qry := range config.Query {

		cond := []Cond{}

		for _, c := range qry.Cond {
			cond = append(cond, Cond{Hdr: c.Header, Val: c.Value, Rel: c.RelaOfCellValue})
		}

		// fmt.Println("Processing ... " + qry.Name)

		QueryFile(
			qry.CsvPath,
			qry.IncCol,
			qry.HdrNames,
			rune(qry.RelaOfCond[0]),
			cond,
			qry.OutCsv,
		)
	}

	return len(config.Query), nil
}
