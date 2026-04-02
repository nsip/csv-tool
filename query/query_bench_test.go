package query

// BenchmarkQueryFileLargeCSV guards against two regressions that were
// introduced when CsvReader switched from a single bulk Write to per-row
// writes:
//
//  1. The bufio.Writer regression (no write buffering): ~2.3M write(2)
//     syscalls for a large file.  Fixed by wrapping fw in bufio.NewWriterSize.
//
//  2. The IsNil-per-row regression: go-generics IsNil falls back to
//     fmt.Sprint(v) which serialises the entire *bufio.Writer struct — including
//     its 1 MiB internal byte slice — via reflection on every single row.
//     For a 500k-row file that is 500k × 1 MB of reflect work ≈ total freeze.
//     Fixed by caching wNotNil := !IsNil(w) once before the hot loop.
//
// Run with:
//
//	go test -run=^$ -bench=BenchmarkQueryFileLargeCSV -benchtime=1x ./query/
import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// generateBenchCSV writes a CSV with nRows data rows to path.
func generateBenchCSV(tb testing.TB, path string, nRows int) {
	tb.Helper()
	f, err := os.Create(path)
	if err != nil {
		tb.Fatalf("generateBenchCSV create: %v", err)
	}
	defer f.Close()

	fmt.Fprintln(f, "School,YrLevel,Domain,StudentId,Score,Band,PathTaken,ItemId,Response,Correct")
	for i := 0; i < nRows; i++ {
		fmt.Fprintf(f, "SCH%04d,%d,Reading,STU%08d,%d,%d,Standard,ITEM%06d,A,1\n",
			i%50, (i%6)+3, i, i%100, (i%10)+1, i%500)
	}
}

func BenchmarkQueryFileLargeCSV(b *testing.B) {
	const nRows = 500_000

	dir := b.TempDir()
	inPath := filepath.Join(dir, "large.csv")
	generateBenchCSV(b, inPath, nRows)

	// Trim three schema columns — same operation as the splitter's trim pass.
	trimCols := []string{"School", "YrLevel", "Domain"}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		outPath := filepath.Join(b.TempDir(), "out.csv")
		if err := QueryFile(inPath, false, trimCols, '&', nil, outPath); err != nil {
			b.Fatalf("QueryFile: %v", err)
		}

		// Sanity: trimmed columns must not appear in the output header.
		raw, err := os.ReadFile(outPath)
		if err != nil {
			b.Fatalf("read output: %v", err)
		}
		hdrLine := strings.SplitN(string(raw), "\n", 2)[0]
		for _, col := range trimCols {
			if strings.Contains(hdrLine, col) {
				b.Fatalf("trimmed column %q still present in output header: %s", col, hdrLine)
			}
		}
	}
}
