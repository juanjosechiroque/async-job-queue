package job

import "errors"

const MaxLines = 20000

var ErrInvalidLineCount = errors.New("lines must be between 1 and 20000")

type PDFGenerator interface {
	Generate(lines int) ([]byte, error)
}

type Service struct {
	pdfGenerator PDFGenerator
}

func NewService(pdfGenerator PDFGenerator) *Service {
	return &Service{pdfGenerator: pdfGenerator}
}

func (s *Service) GeneratePDF(lines int) ([]byte, error) {
	if lines < 1 || lines > MaxLines {
		return nil, ErrInvalidLineCount
	}

	return s.pdfGenerator.Generate(lines)
}
