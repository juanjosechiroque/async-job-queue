package pdf

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

type fixedTextGenerator struct {
	text string
}

func (g fixedTextGenerator) Generate(*rand.Rand) string {
	return g.text
}

func TestEscapePDFText(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string
	}{
		{
			name: "backslash",
			text: `path\to\file`,
			want: `path\\to\\file`,
		},
		{
			name: "parentheses",
			text: "(example)",
			want: `\(example\)`,
		},
		{
			name: "all special characters",
			text: `\(example)`,
			want: `\\\(example\)`,
		},
		{
			name: "plain text",
			text: "plain text",
			want: "plain text",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := escapePDFText(tt.text); got != tt.want {
				t.Errorf("escapePDFText(%q) = %q, want %q", tt.text, got, tt.want)
			}
		})
	}
}

func TestGeneratorGenerateStructure(t *testing.T) {
	tests := []struct {
		name      string
		lines     int
		pageCount int
	}{
		{name: "one line", lines: 1, pageCount: 1},
		{name: "page boundary", lines: 29, pageCount: 1},
		{name: "first line of second page", lines: 30, pageCount: 2},
		{name: "multiple full pages", lines: 100, pageCount: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			document, err := NewGenerator(fixedTextGenerator{text: "fixed text"}).Generate(tt.lines)
			if err != nil {
				t.Fatalf("Generate(%d) returned error: %v", tt.lines, err)
			}
			if !bytes.HasPrefix(document, []byte("%PDF-1.4\n")) {
				t.Errorf("Generate(%d) does not start with PDF header", tt.lines)
			}
			if !bytes.HasSuffix(document, []byte("%%EOF\n")) {
				t.Errorf("Generate(%d) does not end with PDF EOF marker", tt.lines)
			}

			pageObjects := bytes.Count(document, []byte("/Type /Page /Parent 2 0 R"))
			if pageObjects != tt.pageCount {
				t.Errorf("Generate(%d) created %d page objects, want %d", tt.lines, pageObjects, tt.pageCount)
			}
			assertXrefOffsets(t, document)
		})
	}
}

func assertXrefOffsets(t *testing.T, document []byte) {
	t.Helper()

	xrefStart := bytes.LastIndex(document, []byte("\nxref\n"))
	if xrefStart == -1 {
		t.Fatal("PDF does not contain an xref section")
	}

	lines := strings.Split(string(document[xrefStart+1:]), "\n")
	if len(lines) < 2 || lines[0] != "xref" {
		t.Fatal("PDF xref section has an invalid header")
	}

	header := strings.Fields(lines[1])
	if len(header) != 2 || header[0] != "0" {
		t.Fatalf("PDF xref subsection header = %q, want object range starting at 0", lines[1])
	}
	objectCount, err := strconv.Atoi(header[1])
	if err != nil {
		t.Fatalf("PDF xref object count %q is not an integer: %v", header[1], err)
	}
	if len(lines) < objectCount+2 {
		t.Fatalf("PDF xref has %d entries, want %d", len(lines)-2, objectCount)
	}

	for objectNumber := 1; objectNumber < objectCount; objectNumber++ {
		entry := strings.Fields(lines[objectNumber+2])
		if len(entry) != 3 || entry[2] != "n" {
			t.Fatalf("PDF xref entry for object %d = %q, want in-use entry", objectNumber, lines[objectNumber+2])
		}
		offset, err := strconv.Atoi(entry[0])
		if err != nil {
			t.Fatalf("PDF xref offset for object %d (%q) is not an integer: %v", objectNumber, entry[0], err)
		}
		if offset < 0 || offset >= len(document) {
			t.Fatalf("PDF xref offset for object %d is %d, outside document length %d", objectNumber, offset, len(document))
		}

		objectHeader := []byte(fmt.Sprintf("%d 0 obj", objectNumber))
		if !bytes.HasPrefix(document[offset:], objectHeader) {
			t.Errorf("PDF xref offset for object %d points to %q, want %q", objectNumber, document[offset:], objectHeader)
		}
	}
}
