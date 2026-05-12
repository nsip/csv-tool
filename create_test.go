package csvtool

import (
	"fmt"
	"os"
	"testing"

	dt "github.com/nsip/gotk/data-type"
)

func TestCombine(t *testing.T) {
	type args struct {
		csvA            string
		csvB            string
		linkHeaders     []string
		onlyKeepLinkRow bool
		csvOut          string
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{
			name: "OK",
			args: args{
				csvA:            "./data/Modules.csv",
				csvB:            "./data/Questions.csv",
				linkHeaders:     []string{"module_version_id"},
				onlyKeepLinkRow: true,
				csvOut:          "./out/combine.csv",
			},
		},
		// {
		// 	name: "OK",
		// 	args: args{
		// 		csvA:        "./data/Modules.csv",
		// 		csvB:        "./data/Questions.csv",
		// 		linkHeaders:     []string{"module_version_id"},
		// 		onlyKeepLinkRow: false,
		// 		csvOut:          "./out/combine1.csv",
		// 	},
		// },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Combine(tt.args.csvA, tt.args.csvB, tt.args.linkHeaders, tt.args.onlyKeepLinkRow, tt.args.csvOut)
		})
	}
}

func TestAppendOneRowCells(t *testing.T) {
	fPath := "./data/test.csv"

	Create(fPath, "h1", "h2", "h3", "h4")
	fmt.Println(AppendOneRowCells(fPath, true, "c1\",c1", "c2\nc2", ",c2", ",,,", "ignore"))
	fmt.Println(AppendOneRowCells(fPath, true, "N/A"))

	m := make(map[string]any)
	m["h2"] = nil
	m["h3"] = "hello,\t Cell2"
	fmt.Println(AppendOneRowByMap(fPath, true, m, "N/A"))

	data, err := os.ReadFile(fPath)
	if err != nil {
		panic(err)
	}
	if !dt.IsCSV(data) {
		panic("NOT valid CSV output")
	}
}
