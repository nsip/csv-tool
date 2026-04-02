package csvtool

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func TestFileColumn(t *testing.T) {
	type args struct {
		csv string
		idx int
	}
	tests := []struct {
		name      string
		args      args
		wantHdr   string
		wantCells []string
		wantErr   bool
	}{
		{
			// Column() returns the CellEsc-encoded header of the requested column
			// and the raw (CSV-unescaped) cell values for every data row including
			// duplicates. data.csv column 1 ("Name,Name1") has 10 data rows; the
			// duplicates on rows 4-5 and 8-10 are intentional test data.
			name: "OK",
			args: args{
				csv: "./data/data.csv",
				idx: 1,
			},
			wantHdr: `"Name,Name1"`, // CellEsc wraps the header in quotes because it contains a comma
			wantCells: []string{
				`Ahmad,Ahmad`, // row 0 — comma-containing value, unescaped by CSV parser
				`Hello`,       // row 1
				`Test1`,       // row 2
				`Test2`,       // row 3
				`Test1`,       // row 4 (duplicate of row 2)
				`Test2`,       // row 5 (duplicate of row 3)
				`[""abc]`,     // row 6 — [""""abc] in CSV → ["abc] unescaped → [""abc] when re-examined
				`Test2`,       // row 7 (duplicate of row 3)
				`[""abc]`,     // row 8 (duplicate of row 6)
				`[""abc]`,     // row 9 (duplicate of row 6)
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHdr, gotCells, err := FileColumn(tt.args.csv, tt.args.idx)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileColumn() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHdr != tt.wantHdr {
				t.Errorf("FileColumn() gotHdr = %v, want %v", gotHdr, tt.wantHdr)
			}
			if !reflect.DeepEqual(gotCells, tt.wantCells) {
				t.Errorf("FileColumn() gotCells = %v, want %v", gotCells, tt.wantCells)
			}
		})
	}
}

func TestFileColAttr(t *testing.T) {
	type args struct {
		csv string
		idx int
	}
	tests := []struct {
		name    string
		args    args
		want    *ColAttr
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "OK",
			args: args{
				csv: "./data/itemResults999.csv",
				idx: 10,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csv: "./data/Substrands.csv",
				idx: 0,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spew.Dump(FileColAttr(tt.args.csv, tt.args.idx))
		})
	}
}
