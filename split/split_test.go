package split

import (
	"fmt"
	"testing"

	fd "github.com/nsip/gotk/file-dir"
)

func TestSplit(t *testing.T) {
	type args struct {
		csv        string
		out        string
		categories []string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "OK",
			args: args{
				csv:        "./data/qldStudent.csv",
				out:        "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csv:        "./data/sub/itemResults0.csv",
				out:        "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csv:        "./data/sub/itemResults111.csv",
				out:        "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csv:        "./data/sub/itemResults110.csv",
				out:        "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csv:        "./data/sub/itemResults101.csv",
				out:        "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csv:        "/home/qingmiao/Desktop/nrt-issue/csv-tool/data/sub/itemResults100.csv",
				out:        "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		// {
		// 	name: "OK",
		// 	args: args{
		// 		csv:    "./data/data.csv",
		// 		out:     "outmedium",
		// 		categories: []string{"School", "Domain", "YrLevel"},
		// 	},
		// 	want:    []string{},
		// 	wantErr: false,
		// },
		// {
		// 	name: "OK",
		// 	args: args{
		// 		csv:    "./data/big/itemResults.csv",
		// 		out:     "outbig",
		// 		categories: []string{"School", "Domain", "YrLevel"},
		// 	},
		// 	want:    []string{},
		// 	wantErr: false,
		// },
	}

	ForceSglProc(true)
	StrictSchema(true, "")
	RmSchemaCol(true)
	RmSchemaColInIgn(true)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			splitFiles, ignoredFiles, _ := Split(tt.args.csv, tt.args.out, tt.args.categories...)
			fmt.Println(len(splitFiles))
			fmt.Println(len(ignoredFiles))
		})
	}

	fmt.Println(fd.WalkFileDir("out", true))
	fmt.Println(fd.WalkFileDir("outmedium", true))
	fmt.Println(fd.WalkFileDir("outbig", true))
}
