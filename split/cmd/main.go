package main

import "github.com/nsip/csv-tool/split"

func main() {

	split.ForceSglProc(true)
	split.StrictSchema(true, "")
	split.RmSchemaCol(true)
	split.RmSchemaColInIgn(true)

	_, _, err := split.Split("../../data/qldStudent.csv", "./out", "School", "YearLevel")
	if err != nil {
		panic(err)
	}

	_, _, err = split.Split("../../data/qldStudent.csv", "./out", "School", "YearLevel", "Domain") // "Domain" => ignore
	if err != nil {
		panic(err)
	}

	_, _, err = split.Split("../../data/data.csv", "./out", "School", "YearLevel", "Domain") // ignore
	if err != nil {
		panic(err)
	}

}
