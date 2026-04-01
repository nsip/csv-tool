package split

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	ct "github.com/nsip/csv-tool"
	qry "github.com/nsip/csv-tool/query"
	. "github.com/digisan/go-generics"
	fd "github.com/digisan/gotk/file-dir"
	lk "github.com/digisan/logkit"
)

var (
	basename         string
	csvDir           string
	mtx              = &sync.Mutex{}
	schema           []string
	nSchema          int
	rmSchemaCol      bool
	rmSchemaColInIgn bool
	outDir           string
	splitFiles       []string
	ignoredFiles     []string
	parallel         = false
	sglProc          = false
	ignoredOut       = "ignored"
	strictSchema     = false
)

// RmSchemaCol :
func RmSchemaCol(rmSchema bool) {
	rmSchemaCol = rmSchema
}

// RmSchemaColInIgn :
func RmSchemaColInIgn(rmSchema bool) {
	rmSchemaColInIgn = rmSchema
}

// StrictSchema :
func StrictSchema(strict bool, ignrOut string) {
	strictSchema = strict
	if ignrOut != "" {
		ignoredOut = ignrOut
	}
}

// ForceSglProc :
func ForceSglProc(sp bool) {
	sglProc = sp
}

// Split : return (splitFiles, ignoredFiles, error)
func Split(csv, out string, categories ...string) ([]string, []string, error) {

	basename, csvDir = filepath.Base(csv), filepath.Dir(csv)
	schema, nSchema = categories, len(categories)

	if out == "" {
		outDir = "./" + strings.TrimSuffix(basename, ".csv") + "/"
	} else {
		outDir = strings.TrimSuffix(out, "/") + "/"
	}

	if !fd.DirExists(outDir) {
		fd.MustCreateDir(outDir)
	}

	in, err := os.ReadFile(csv)
	if err != nil {
		return nil, nil, fmt.Errorf("%v @ %s", err, csv)
	}

	// --------------- strict schema check --------------- //
	headers, nRow, err := ct.FileInfo(csv)
	if err != nil {
		return nil, nil, fmt.Errorf("%v @ %s", err, csv)
	}
	if strictSchema {
		if !IsSuper(headers, schema) || nRow == 0 {

			nsCsv := filepath.Clean(filepath.Join(out, ignoredOut, basename))

			if rmSchemaColInIgn {
				fd.MustCreateDir(filepath.Dir(nsCsv))
				fw, err := os.OpenFile(nsCsv, os.O_WRONLY|os.O_CREATE, 0666)
				if err != nil {
					return nil, nil, err
				}
				qry.Subset(in, false, schema, false, nil, fw)
				fw.Close()
			} else {
				fd.MustWriteFile(nsCsv, in)
			}

			return []string{}, []string{nsCsv}, nil
		}
	}

	// --------------- parallel set --------------- //
	parallel = false
	if !sglProc && len(in) < 1024*1024*10 {
		parallel = true
	}
	// fmt.Printf("%s running on parallel? %v\n", csvFile, parallel)

	splitFiles, ignoredFiles = []string{}, []string{}
	return splitFiles, ignoredFiles, split(0, in, outDir)
}

func split(rl int, in []byte, prevPath string, pCatCells ...string) error {

	if rl >= nSchema {
		return nil
	}

	cat := schema[rl]
	rl++

	rmHdrGrp := []string{cat}
	if !rmSchemaCol {
		rmHdrGrp = nil
	}

	_, rows, err := qry.Subset(in, true, []string{cat}, false, nil, nil)
	if err != nil {
		return err
	}

	// --------------- not splittable --------------- //
	// empty / empty content / missing needed categories
	if func() bool {

		mtx.Lock()
		defer mtx.Unlock()

		if len(rows) == 0 || (len(rows) > 0 && len(strings.Trim(rows[0], " \t")) == 0) {

			ignoredOutInfo := fmt.Sprintf("%s(missing %s)", filepath.Base(ignoredOut), cat)
			nsCsvDir, _ := fd.RelPath(csvDir, false)
			ignoredInfo := fmt.Sprintf("%s(%s).csv", strings.TrimSuffix(basename, ".csv"), nsCsvDir)
			ignoredInfo = strings.ReplaceAll(ignoredInfo, "/", "~")
			nsCsv := filepath.Join(prevPath, ignoredOutInfo, ignoredInfo)

			if rmSchemaColInIgn {
				fd.MustCreateDir(filepath.Dir(nsCsv))
				fw, err := os.OpenFile(nsCsv, os.O_WRONLY|os.O_CREATE, 0666)
				if err != nil {
					lk.Warn("%v", err)
					return true
				}
				qry.Subset(in, false, schema, false, nil, fw)
				fw.Close()
			} else {
				fd.MustWriteFile(nsCsv, in)
			}

			ignoredFiles = append(ignoredFiles, nsCsv)
			return true
		}
		return false

	}() {
		return nil
	}

	// --------------- end --------------- //

	uniRows := Settify(rows...)
	FilterFast(&uniRows, func(i int, e string) bool { return len(strings.TrimSpace(e)) > 0 })

	// Safe Mode, But Slow //
	if !parallel {

		for _, catCell := range uniRows {

			csvOut := outDir
			for _, pcCell := range pCatCells {
				csvOut += pcCell + "/"
			}
			csvOut += catCell + "/" + basename

			wBuf := &bytes.Buffer{}

			qry.Query(
				in,
				false,
				rmHdrGrp,
				'&',
				[]qry.Cond{{Hdr: cat, Val: catCell, Rel: "="}},
				io.Writer(wBuf),
			)

			if rl == nSchema {
				fd.MustWriteFile(csvOut, wBuf.Bytes())
				splitFiles = append(splitFiles, csvOut)
			}

			split(rl, wBuf.Bytes(), filepath.Dir(csvOut), append(pCatCells, catCell)...)
		}
	}

	// Whole Linux Exhausted When Running On Big Data //
	if parallel {

		wg := &sync.WaitGroup{}
		wg.Add(len(uniRows))

		for _, catCell := range uniRows {

			go func(catCell string) {
				defer wg.Done()

				csvOut := outDir
				for _, pcCell := range pCatCells {
					csvOut += pcCell + "/"
				}
				csvOut += catCell + "/" + basename

				wBuf := &bytes.Buffer{}

				qry.Query(
					in,
					false,
					rmHdrGrp,
					'&',
					[]qry.Cond{{Hdr: cat, Val: catCell, Rel: "="}},
					io.Writer(wBuf),
				)

				if rl == nSchema {
					mtx.Lock()
					fd.MustWriteFile(csvOut, wBuf.Bytes())
					splitFiles = append(splitFiles, csvOut)
					mtx.Unlock()
				}

				split(rl, wBuf.Bytes(), filepath.Dir(csvOut), append(pCatCells, catCell)...)

			}(catCell)
		}

		wg.Wait()
	}

	return nil
}
