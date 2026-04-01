package query

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	ct "github.com/nsip/csv-tool"
	. "github.com/digisan/go-generics"
	fd "github.com/digisan/gotk/file-dir"
	"github.com/digisan/gotk/track"
	lk "github.com/digisan/logkit"
)

func TestSliceEq(t *testing.T) {
	s1 := []int{1, 2, 3}
	s2 := []int{1, 2, 3}
	fmt.Println(reflect.DeepEqual(s1, s2))
}

func TestUnique(t *testing.T) {

	defer track.TrackTime(time.Now())
	lk.Log2F(true, true, "./TestSubset.log")

	Unique("../data/data.csv", "../out/data-uni.csv")
	GetRepeated("../data/data.csv", "../out/data-rep.csv", func(rRepCnt int) bool { return rRepCnt >= 2 })
}

func TestSubset(t *testing.T) {

	defer track.TrackTime(time.Now())
	lk.Log2F(true, true, "./TestSubset.log")

	dir := "../data/"
	files, err := os.ReadDir(dir)
	lk.FailOnErr("%v", err)

	for _, file := range files {
		fName := filepath.Join(dir, file.Name())
		if !strings.HasSuffix(file.Name(), ".csv") {
			continue
		}
		// if file.Name() != "itemResults1.csv" {
		// 	continue
		// }

		func() {

			fmt.Println(fName)
			_, n, _ := ct.FileInfo(fName)

			in, err := os.ReadFile(fName)
			lk.FailOnErr("%v", err)

			out := "subset-out/"
			fd.MustCreateDir(out)
			file4w, err := os.OpenFile(filepath.Join(out, file.Name()), os.O_WRONLY|os.O_CREATE, 0644)
			lk.FailOnErr("%v", err)
			defer file4w.Close()
			Subset(
				in,
				false,
				[]string{"Domain", "Item Response", "YrLevel", "School", "Age", "substrand_id"},
				true,
				IterToSlc(n-1, -1),
				file4w,
			)

			out1 := "subset-out1/"
			fd.MustCreateDir(out1)
			file4w1, err := os.OpenFile(filepath.Join(out1, file.Name()), os.O_WRONLY|os.O_CREATE, 0644)
			lk.FailOnErr("%v", err)
			defer file4w1.Close()
			Subset(
				in,
				true,
				[]string{"School", "Domain", "YrLevel", "XXX", "Test Name", "Test level", "Test Domain", "Test Item RefID", "Item Response"},
				true,
				IterToSlc(0, 20000),
				file4w1,
			)
		}()
	}
}

func TestSelect(t *testing.T) {

	defer track.TrackTime(time.Now())
	lk.Log2F(true, true, "./TestSelect.log")

	dir := "../data/"
	files, err := os.ReadDir(dir)
	lk.FailOnErr("%v", err)

	for _, file := range files {
		fName := filepath.Join(dir, file.Name())

		// specific file
		if fName != "data.csv" {
			return
		}

		if !strings.HasSuffix(fName, ".csv") {
			continue
		}

		fmt.Println(fName)

		func() {

			in, err := os.ReadFile(fName)
			lk.FailOnErr("%v", err)

			fd.MustWriteFile("out/"+file.Name(), []byte{})
			file4w, err := os.OpenFile("out/"+file.Name(), os.O_WRONLY|os.O_CREATE, 0666)
			lk.FailOnErr("%v", err)
			defer file4w.Close()

			Select(in, '&', []Cond{
				{Hdr: "School", Val: "21221", Rel: "="},
				{Hdr: "Domain", Val: "Spelling", Rel: "="},
				{Hdr: "YrLevel", Val: 3, Rel: "<="},
			}, file4w)

		}()
	}
}

func TestQuery(t *testing.T) {

	defer track.TrackTime(time.Now())
	lk.Log2F(true, true, "./TestQuery.log")

	dir := "../data"
	files, err := os.ReadDir(dir)
	lk.FailOnErr("%v", err)

	n := len(files)
	fmt.Println(n, "files")

	wg := &sync.WaitGroup{}
	wg.Add(n)

	for _, file := range files {

		go func(filename string) {
			defer wg.Done()

			// specific file
			// if filename != "data.csv" {
			// 	return
			// }

			if !strings.HasSuffix(filename, ".csv") {
				return
			}

			fName := filepath.Join(dir, filename)
			fmt.Println(fName)

			QueryFile(
				fName,
				true,
				[]string{
					"Domain",
					"School",
					"YrLevel",
					"Test Name",
					"Test level",
					"Test Domain",
					"Test Item RefID",
				},
				'|',
				[]Cond{
					{Hdr: "School", Val: "21221", Rel: "="},
					{Hdr: "YrLevel", Val: 5, Rel: ">"},
					{Hdr: "Domain", Val: "Reading", Rel: "="},
				},
				"out/"+filename,
			)

		}(file.Name())
	}

	wg.Wait()

	fmt.Println(fd.WalkFileDir(dir, true))
	fmt.Println(fd.WalkFileDir("out/", true))
}

func TestQueryByConfig(t *testing.T) {
	n, err := QueryByConfig("./query.toml")
	lk.FailOnErr("%v", err)
	fmt.Println(n)
}
