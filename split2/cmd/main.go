package main

import (
	"fmt"
	"time"

	"github.com/nsip/csv-tool/split2"
	"github.com/digisan/gotk/track"
)

// func main() {

// 	split2.StrictSchema(true, "")
// 	split2.RmSchemaCol(true)
// 	split2.RmSchemaColInIgn(false)

// 	_, ignFiles, err := split2.Split("../../data/qldStudent.csv", "./out", "School", "YearLevel")
// 	if err != nil {
// 		panic(err)
// 	}
// 	if len(ignFiles) > 0 {
// 		fmt.Println("cannot be split, ignored")
// 	}

// 	fmt.Println(" -------------------------- ")

// 	_, ignFiles, err = split2.Split("../../data/qldStudent.csv", "./out", "School", "YearLevel", "Domain") // "Domain" => ignore
// 	if err != nil {
// 		panic(err)
// 	}
// 	if len(ignFiles) > 0 {
// 		fmt.Println("cannot be split, ignored")
// 	}

// 	fmt.Println(" -------------------------- ")

// 	_, ignFiles, err = split2.Split("../../data/data.csv", "./out", "School", "YearLevel", "Domain") // ignore
// 	if err != nil {
// 		panic(err)
// 	}
// 	if len(ignFiles) > 0 {
// 		fmt.Println("cannot be split, ignored")
// 	}
// }

func main() {

	defer track.TrackTime(time.Now())

	split2.StrictSchema(true, "")
	split2.RmSchemaCol(false)
	split2.RmSchemaColInIgn(false)

	_, ignFiles, err := split2.Split("../../data/sample-small.csv", "./out", "UserID")
	if err != nil {
		panic(err)
	}
	if len(ignFiles) > 0 {
		fmt.Println("cannot be split, ignored")
	}

}
