package pdf

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type TextGenerator interface {
	Generate(rng *rand.Rand) string
}

type Generator struct {
	textGenerator TextGenerator
}

func NewGenerator(textGenerator TextGenerator) *Generator {
	return &Generator{textGenerator: textGenerator}
}

func (g *Generator) Generate(lines int) ([]byte, error) {
	var pdf bytes.Buffer
	pdf.WriteString("%PDF-1.4\n")
	pdf.Write([]byte{'%', 0xe2, 0xe3, 0xcf, 0xd3, '\n'})

	const linesPerPage = 29
	pageCount := (lines + linesPerPage - 1) / linesPerPage
	offsets := []int{0}
	addObject := func(number int, body string) {
		offsets = append(offsets, pdf.Len())
		fmt.Fprintf(&pdf, "%d 0 obj\n%s\nendobj\n", number, body)
	}

	addObject(1, "<< /Type /Catalog /Pages 2 0 R >>")

	pageIDs := make([]int, pageCount)
	pageReferences := make([]string, pageCount)
	for i := range pageIDs {
		pageIDs[i] = 4 + i*2
		pageReferences[i] = strconv.Itoa(pageIDs[i]) + " 0 R"
	}
	addObject(2, "<< /Type /Pages /Kids ["+strings.Join(pageReferences, " ")+"] /Count "+strconv.Itoa(pageCount)+" >>")
	addObject(3, "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for page := 0; page < pageCount; page++ {
		pageID := pageIDs[page]
		contentID := pageID + 1
		firstLine := page*linesPerPage + 1
		lastLine := firstLine + linesPerPage - 1
		if lastLine > lines {
			lastLine = lines
		}

		var content strings.Builder
		content.WriteString("BT\n/F1 14 Tf\n72 740 Td\n")
		for line := firstLine; line <= lastLine; line++ {
			text := g.textGenerator.Generate(rng)
			fmt.Fprintf(&content, "(%s) Tj\n", escapePDFText(fmt.Sprintf("%d. %s", line, text)))
			if line != lastLine {
				content.WriteString("0 -24 Td\n")
			}
		}
		content.WriteString("ET")

		addObject(pageID, "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 3 0 R >> >> /Contents "+strconv.Itoa(contentID)+" 0 R >>")
		addObject(contentID, "<< /Length "+strconv.Itoa(content.Len())+" >>\nstream\n"+content.String()+"\nendstream")
	}

	xrefOffset := pdf.Len()
	fmt.Fprintf(&pdf, "xref\n0 %d\n0000000000 65535 f \n", len(offsets))
	for _, offset := range offsets[1:] {
		fmt.Fprintf(&pdf, "%010d 00000 n \n", offset)
	}
	fmt.Fprintf(&pdf, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(offsets), xrefOffset)

	return pdf.Bytes(), nil
}

func escapePDFText(text string) string {
	text = strings.ReplaceAll(text, `\`, `\\`)
	text = strings.ReplaceAll(text, "(", `\(`)
	return strings.ReplaceAll(text, ")", `\)`)
}
