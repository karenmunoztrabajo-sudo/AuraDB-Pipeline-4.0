package service

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/ledongthuc/pdf"
)

var ErrUnsupportedFormat = errors.New("formato no soportado")
var ErrNoExtractableText = errors.New("documento sin texto extraíble")

type ParseResult struct {
	Content      string
	DetectedType string
	Extension    string
	ParserName   string
	PageTexts    []PageText
	ExcelData    *ExcelDocument
}

type PageText struct {
	PageNumber int
	Content    string
}

func ParseDocument(filename string, mimeType string, data []byte) (ParseResult, error) {
	detectedType := detectDocumentType(filename, mimeType)
	extension := strings.ToLower(filepath.Ext(filename))

	switch detectedType {
	case "txt":
		return ParseResult{
			Content:      cleanExtractedText(string(data)),
			DetectedType: detectedType,
			Extension:    extension,
			ParserName:   "plain_text",
		}, nil
	case "csv":
		content, err := parseCSV(data)
		if err != nil {
			return ParseResult{
				DetectedType: detectedType,
				Extension:    extension,
				ParserName:   "csv_table",
			}, err
		}
		return ParseResult{
			Content:      content,
			DetectedType: detectedType,
			Extension:    extension,
			ParserName:   "csv_table",
		}, nil
	case "pdf":
		content, pageTexts, parserName, err := parsePDF(data)
		if err != nil {
			return ParseResult{
				DetectedType: detectedType,
				Extension:    extension,
				ParserName:   "pdf_text",
			}, err
		}
		return ParseResult{
			Content:      content,
			DetectedType: detectedType,
			Extension:    extension,
			ParserName:   parserName,
			PageTexts:    pageTexts,
		}, nil
	case "image":
		content, err := parseImageOCR(data, extension)
		if err != nil {
			return ParseResult{
				DetectedType: detectedType,
				Extension:    extension,
				ParserName:   "image_ocr",
			}, err
		}
		return ParseResult{
			Content:      content,
			DetectedType: detectedType,
			Extension:    extension,
			ParserName:   "image_ocr",
			PageTexts:    []PageText{{PageNumber: 1, Content: content}},
		}, nil
	case "docx":
		content, err := parseDOCX(data)
		if err != nil {
			return ParseResult{
				DetectedType: detectedType,
				Extension:    extension,
				ParserName:   "docx_xml",
			}, err
		}
		return ParseResult{
			Content:      content,
			DetectedType: detectedType,
			Extension:    extension,
			ParserName:   "docx_xml",
		}, nil
	case "xlsx":
		excelDoc, content, err := parseStructuredXLSX(data)
		if err != nil {
			return ParseResult{
				DetectedType: detectedType,
				Extension:    extension,
				ParserName:   "xlsx_excelize",
			}, err
		}
		return ParseResult{
			Content:      content,
			DetectedType: detectedType,
			Extension:    extension,
			ParserName:   "xlsx_excelize",
			ExcelData:    excelDoc,
		}, nil
	default:
		return ParseResult{
			DetectedType: detectedType,
			Extension:    extension,
			ParserName:   "unsupported",
		}, ErrUnsupportedFormat
	}
}

func detectDocumentType(filename string, mimeType string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".txt":
		return "txt"
	case ".csv":
		return "csv"
	case ".pdf":
		return "pdf"
	case ".docx":
		return "docx"
	case ".xlsx":
		return "xlsx"
	case ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tif", ".tiff":
		return "image"
	}

	mimeType = strings.ToLower(strings.TrimSpace(strings.Split(mimeType, ";")[0]))
	switch mimeType {
	case "text/plain":
		return "txt"
	case "text/csv", "application/csv", "application/vnd.ms-excel":
		return "csv"
	case "application/pdf":
		return "pdf"
	case "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
		return "docx"
	case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
		return "xlsx"
	case "image/jpeg", "image/png", "image/gif", "image/webp", "image/bmp", "image/tiff":
		return "image"
	default:
		if strings.HasPrefix(mimeType, "image/") {
			return "image"
		}
		if mimeType == "" {
			return "unknown"
		}
		return mimeType
	}
}

func parseCSV(data []byte) (string, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	records, err := reader.ReadAll()
	if err != nil {
		return "", fmt.Errorf("error parseando csv: %w", err)
	}
	if len(records) == 0 {
		return "", ErrNoExtractableText
	}

	var builder strings.Builder
	for _, record := range records {
		cells := make([]string, 0, len(record))
		for _, cell := range record {
			cell = cleanExtractedText(cell)
			cell = strings.TrimSpace(cell)
			if cell == "" {
				continue
			}
			cells = append(cells, cell)
		}
		if len(cells) == 0 {
			continue
		}
		builder.WriteString(strings.Join(cells, " | "))
		builder.WriteString("\n")
	}

	content := cleanExtractedText(builder.String())
	if content == "" {
		return "", ErrNoExtractableText
	}
	return content, nil
}

func parseDOCX(data []byte) (string, error) {
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return "", fmt.Errorf("error abriendo docx: %w", err)
	}

	documentParts := make([]*zip.File, 0)
	for _, file := range reader.File {
		if isDOCXTextPart(file.Name) {
			documentParts = append(documentParts, file)
		}
	}

	sort.SliceStable(documentParts, func(i, j int) bool {
		return docxPartOrder(documentParts[i].Name) < docxPartOrder(documentParts[j].Name)
	})

	var builder strings.Builder
	for _, file := range documentParts {
		rc, err := file.Open()
		if err != nil {
			return "", fmt.Errorf("error leyendo %s: %w", file.Name, err)
		}

		content, err := extractDOCXText(rc)
		_ = rc.Close()
		if err != nil {
			return "", err
		}
		if content != "" {
			if builder.Len() > 0 {
				builder.WriteString("\n")
			}
			builder.WriteString(content)
		}
	}

	content := strings.TrimSpace(builder.String())
	if content == "" {
		return "", ErrNoExtractableText
	}

	return cleanExtractedText(content), nil
}

func isDOCXTextPart(name string) bool {
	if name == "word/document.xml" ||
		name == "word/footnotes.xml" ||
		name == "word/endnotes.xml" ||
		strings.HasPrefix(name, "word/header") && strings.HasSuffix(name, ".xml") ||
		strings.HasPrefix(name, "word/footer") && strings.HasSuffix(name, ".xml") {
		return true
	}
	return false
}

func docxPartOrder(name string) int {
	switch {
	case name == "word/document.xml":
		return 0
	case strings.HasPrefix(name, "word/header"):
		return 1
	case strings.HasPrefix(name, "word/footer"):
		return 2
	case name == "word/footnotes.xml":
		return 3
	case name == "word/endnotes.xml":
		return 4
	default:
		return 9
	}
}

func extractDOCXText(r io.Reader) (string, error) {
	decoder := xml.NewDecoder(r)
	var builder strings.Builder

	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("error parseando document.xml: %w", err)
		}

		switch t := token.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "t":
				var text string
				if err := decoder.DecodeElement(&text, &t); err != nil {
					return "", fmt.Errorf("error leyendo texto docx: %w", err)
				}
				if text == "" {
					continue
				}
				builder.WriteString(text)
			case "tab":
				builder.WriteString(" ")
			case "br", "p":
				if builder.Len() > 0 {
					builder.WriteString("\n")
				}
			}
		}
	}

	return strings.TrimSpace(builder.String()), nil
}

func parseXLSX(data []byte) (string, error) {
	_, content, err := parseStructuredXLSX(data)
	if err != nil {
		return "", err
	}
	return content, nil
}

func readZipFile(file *zip.File) ([]byte, error) {
	rc, err := file.Open()
	if err != nil {
		return nil, fmt.Errorf("error abriendo %s: %w", file.Name, err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("error leyendo %s: %w", file.Name, err)
	}
	return data, nil
}

type pdfRow struct {
	Position int64
	Text     string
}

type pdfPageContent struct {
	Rows []pdfRow
}

type textCleaningStats struct {
	RawTextLength          int
	CleanedTextLength      int
	RemovedNoiseLinesCount int
	FallbackUsed           bool
}

var (
	inlineWhitespacePattern       = regexp.MustCompile(`[\t\f\v\p{Zs}]+`)
	extraBlankLinesPattern        = regexp.MustCompile(`\n{3,}`)
	spaceBeforePunctPattern       = regexp.MustCompile(`\s+([,.;:!?])`)
	missingSpaceAfterPunctPattern = regexp.MustCompile(`([,.;:!?])([A-Za-zÁÉÍÓÚÑáéíóúñ])`)
	spaceAroundParenPattern       = regexp.MustCompile(`\(\s+|\s+\)`)
	spacedSlashPattern            = regexp.MustCompile(`\s*([/\\|])\s*`)
	repeatedDashSpacePattern      = regexp.MustCompile(`\s*([=-]{2,})\s*`)
	headerFooterDigitsPattern     = regexp.MustCompile(`\d+`)
	isbnPattern                   = regexp.MustCompile(`(?i)\bisbn(?:-1[03])?\b`)
	yearPattern                   = regexp.MustCompile(`(?:19|20)\d{2}`)
	authorSeparatorPattern        = regexp.MustCompile(`\s*(?:,|;| y | and |&)\s*`)
)

func parsePDF(data []byte) (string, []PageText, string, error) {
	pdftotextPath, lookErr := exec.LookPath("pdftotext")
	externalParserAvailable := lookErr == nil
	log.Printf("pdf_external_parser_available=%t", externalParserAvailable)
	if externalParserAvailable {
		externalText, err := parsePDFWithPDFToText(data, pdftotextPath)
		if err == nil && strings.TrimSpace(externalText) != "" && pdfTextQualityScore(externalText) >= 45 {
			selectedContent := cleanExtractedText(externalText)
			selectedQuality := pdfTextQualityScore(selectedContent)
			log.Printf("pdf_parser_used=pdftotext")
			log.Printf("pdf_text_quality_score=%d", selectedQuality)
			log.Printf("pdf_text_preview=%q", pdfTextPreview(selectedContent, 300))
			return selectedContent, nil, "pdf_text_pdftotext", nil
		}
		if err != nil {
			log.Printf("pdf_external_parser_error=%q", err)
		} else {
			log.Printf("pdf_external_parser_error=%q", "empty output")
		}
	}

	ocrText, ocrPages, ocrErr := parsePDFWithOCR(data)
	if ocrErr == nil && strings.TrimSpace(ocrText) != "" {
		selectedContent := cleanExtractedText(ocrText)
		selectedQuality := pdfTextQualityScore(selectedContent)
		log.Printf("pdf_parser_used=ocr")
		log.Printf("pdf_text_quality_score=%d", selectedQuality)
		log.Printf("pdf_text_preview=%q", pdfTextPreview(selectedContent, 300))
		return selectedContent, ocrPages, "pdf_ocr", nil
	}
	if ocrErr != nil {
		log.Printf("pdf_ocr_unavailable_or_failed=true error=%q", ocrErr)
	}

	reader := bytes.NewReader(data)
	pdfReader, err := pdf.NewReader(reader, int64(len(data)))
	if err != nil {
		return "", nil, "pdf_text_internal", fmt.Errorf("error abriendo pdf: %w", err)
	}

	pageCount := pdfReader.NumPage()
	if pageCount == 0 {
		return "", nil, "pdf_text_internal", ErrNoExtractableText
	}

	fonts := make(map[string]*pdf.Font)
	pages := make([]pdfPageContent, 0, pageCount)
	pageTexts := make([]PageText, 0, pageCount)
	for pageIndex := 1; pageIndex <= pageCount; pageIndex++ {
		page := pdfReader.Page(pageIndex)
		if page.V.IsNull() {
			continue
		}

		for _, name := range page.Fonts() {
			if _, ok := fonts[name]; ok {
				continue
			}
			font := page.Font(name)
			fonts[name] = &font
		}

		rows, err := page.GetTextByRow()
		if err == nil {
			pageRows := make([]pdfRow, 0, len(rows))
			for _, row := range rows {
				text := normalizePDFLine(joinPDFRow(row))
				if text == "" {
					continue
				}
				pageRows = append(pageRows, pdfRow{
					Position: row.Position,
					Text:     text,
				})
			}
			pages = append(pages, pdfPageContent{Rows: pageRows})
			if pageText := buildPDFFromPages([]pdfPageContent{{Rows: pageRows}}); pageText != "" {
				pageTexts = append(pageTexts, PageText{PageNumber: pageIndex, Content: pageText})
			}
			continue
		}

		fallbackText, fallbackErr := page.GetPlainText(fonts)
		if fallbackErr != nil {
			return "", nil, "pdf_text_internal", fmt.Errorf("error extrayendo texto pdf: %w", fallbackErr)
		}
		pageRows := make([]pdfRow, 0)
		for _, line := range strings.Split(fallbackText, "\n") {
			line = normalizePDFLine(line)
			if line == "" {
				continue
			}
			pageRows = append(pageRows, pdfRow{Text: line})
		}
		pages = append(pages, pdfPageContent{Rows: pageRows})
		if pageText := buildPDFFromPages([]pdfPageContent{{Rows: pageRows}}); pageText != "" {
			pageTexts = append(pageTexts, PageText{PageNumber: pageIndex, Content: pageText})
		}
	}

	rawContent := buildPDFFromPages(pages)
	if rawContent == "" {
		return "", nil, "pdf_text_internal", ErrNoExtractableText
	}

	content, stats := cleanExtractedTextWithStats(rawContent)
	if strings.TrimSpace(content) == "" && rawContent != "" {
		content = strings.TrimSpace(rawContent)
		stats.CleanedTextLength = len(content)
		stats.FallbackUsed = true
	}

	selectedContent := strings.TrimSpace(content)
	selectedParser := "internal"
	selectedQuality := pdfTextQualityScore(selectedContent)

	log.Printf(
		"pdf_text_cleaned raw_text_length=%d cleaned_text_length=%d removed_noise_lines_count=%d fallback_used=%t",
		stats.RawTextLength,
		stats.CleanedTextLength,
		stats.RemovedNoiseLinesCount,
		stats.FallbackUsed,
	)
	log.Printf("pdf_parser_used=%s", selectedParser)
	log.Printf("pdf_text_quality_score=%d", selectedQuality)
	log.Printf("pdf_text_preview=%q", pdfTextPreview(selectedContent, 300))

	if strings.TrimSpace(selectedContent) == "" {
		return "", nil, selectedParser, ErrNoExtractableText
	}

	return selectedContent, pageTexts, "pdf_text_internal", nil
}

func parsePDFWithPDFToText(data []byte, pdftotextPath string) (string, error) {
	tmpFile, err := os.CreateTemp("", "auradb-pdf-*.pdf")
	if err != nil {
		return "", err
	}
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return "", err
	}
	if err := tmpFile.Close(); err != nil {
		return "", err
	}

	cmd := exec.Command(pdftotextPath, "-layout", tmpName, "-")
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(output), nil
}

func parsePDFWithOCR(data []byte) (string, []PageText, error) {
	pdftoppmPath, err := exec.LookPath("pdftoppm")
	if err != nil {
		return "", nil, fmt.Errorf("pdftoppm no disponible: %w", err)
	}
	tesseractPath, err := exec.LookPath("tesseract")
	if err != nil {
		return "", nil, fmt.Errorf("tesseract no disponible: %w", err)
	}

	tmpDir, err := os.MkdirTemp("", "auradb-pdf-ocr-*")
	if err != nil {
		return "", nil, err
	}
	defer os.RemoveAll(tmpDir)

	pdfPath := filepath.Join(tmpDir, "input.pdf")
	if err := os.WriteFile(pdfPath, data, 0600); err != nil {
		return "", nil, err
	}

	prefix := filepath.Join(tmpDir, "page")
	cmd := exec.Command(pdftoppmPath, "-png", "-r", "200", pdfPath, prefix)
	if output, err := cmd.CombinedOutput(); err != nil {
		return "", nil, fmt.Errorf("pdftoppm error: %w output=%s", err, strings.TrimSpace(string(output)))
	}

	imagePaths, err := filepath.Glob(prefix + "-*.png")
	if err != nil {
		return "", nil, err
	}
	sort.Strings(imagePaths)
	if len(imagePaths) == 0 {
		return "", nil, ErrNoExtractableText
	}

	pageTexts := make([]PageText, 0, len(imagePaths))
	var builder strings.Builder
	for i, imagePath := range imagePaths {
		text, err := runTesseractImage(tesseractPath, imagePath)
		if err != nil {
			log.Printf("pdf_ocr_page_error page=%d error=%v", i+1, err)
			continue
		}
		text = cleanExtractedText(text)
		if text == "" {
			continue
		}
		if builder.Len() > 0 {
			builder.WriteString("\n\n")
		}
		builder.WriteString(text)
		pageTexts = append(pageTexts, PageText{PageNumber: i + 1, Content: text})
	}

	content := strings.TrimSpace(builder.String())
	if content == "" {
		return "", nil, ErrNoExtractableText
	}
	return content, pageTexts, nil
}

func parseImageOCR(data []byte, extension string) (string, error) {
	tesseractPath, err := exec.LookPath("tesseract")
	if err != nil {
		return "", fmt.Errorf("tesseract no disponible: %w", err)
	}

	if extension == "" {
		extension = ".img"
	}
	tmpFile, err := os.CreateTemp("", "auradb-image-*"+extension)
	if err != nil {
		return "", err
	}
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return "", err
	}
	if err := tmpFile.Close(); err != nil {
		return "", err
	}

	text, err := runTesseractImage(tesseractPath, tmpName)
	if err != nil {
		return "", err
	}
	text = cleanExtractedText(text)
	if text == "" {
		return "", ErrNoExtractableText
	}
	return text, nil
}

func runTesseractImage(tesseractPath string, imagePath string) (string, error) {
	cmd := exec.Command(tesseractPath, imagePath, "stdout", "-l", "spa+eng")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("tesseract error: %w output=%s", err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}

func joinPDFRow(row *pdf.Row) string {
	if row == nil || len(row.Content) == 0 {
		return ""
	}

	var builder strings.Builder
	var previousEnd float64
	for index, text := range row.Content {
		segment := cleanInlineText(text.S)
		if segment == "" {
			continue
		}

		if index > 0 && builder.Len() > 0 {
			gap := text.X - previousEnd
			if shouldMergePDFSegments(builder.String(), segment, gap) {
				// Keep the fragments together when the PDF extractor split a single word.
			} else if shouldInsertSpaceBetweenPDFSegments(builder.String(), segment, gap) {
				builder.WriteByte(' ')
			}
		}

		builder.WriteString(segment)
		previousEnd = text.X + text.W
	}

	return builder.String()
}

func buildPDFFromPages(pages []pdfPageContent) string {
	if len(pages) == 0 {
		return ""
	}

	headerCandidates, footerCandidates := detectRepeatedPageArtifacts(pages)
	blocks := make([]string, 0)

	for _, page := range pages {
		filteredRows := filterPageArtifacts(page.Rows, headerCandidates, footerCandidates)
		pageBlocks := rowsToBlocks(filteredRows)
		blocks = append(blocks, pageBlocks...)
	}

	return strings.TrimSpace(strings.Join(blocks, "\n\n"))
}

func detectRepeatedPageArtifacts(pages []pdfPageContent) (map[string]struct{}, map[string]struct{}) {
	headerCounts := make(map[string]int)
	footerCounts := make(map[string]int)

	for _, page := range pages {
		limit := min(2, len(page.Rows))
		for i := 0; i < limit; i++ {
			key := normalizeRepeatedLineKey(page.Rows[i].Text)
			if key != "" {
				headerCounts[key]++
			}
		}
		for i := len(page.Rows) - limit; i < len(page.Rows); i++ {
			if i < 0 {
				continue
			}
			key := normalizeRepeatedLineKey(page.Rows[i].Text)
			if key != "" {
				footerCounts[key]++
			}
		}
	}

	threshold := 2
	if len(pages) >= 4 {
		threshold = (len(pages) + 1) / 2
	}

	headers := make(map[string]struct{})
	footers := make(map[string]struct{})

	for key, count := range headerCounts {
		if count >= threshold {
			headers[key] = struct{}{}
		}
	}
	for key, count := range footerCounts {
		if count >= threshold {
			footers[key] = struct{}{}
		}
	}

	return headers, footers
}

func filterPageArtifacts(rows []pdfRow, headerCandidates map[string]struct{}, footerCandidates map[string]struct{}) []pdfRow {
	if len(rows) == 0 {
		return nil
	}

	filtered := make([]pdfRow, 0, len(rows))
	headerLimit := min(2, len(rows))
	footerStart := max(0, len(rows)-2)

	for index, row := range rows {
		key := normalizeRepeatedLineKey(row.Text)
		if key != "" {
			if index < headerLimit {
				if _, ok := headerCandidates[key]; ok {
					continue
				}
			}
			if index >= footerStart {
				if _, ok := footerCandidates[key]; ok {
					continue
				}
			}
		}
		filtered = append(filtered, row)
	}

	return filtered
}

func rowsToBlocks(rows []pdfRow) []string {
	if len(rows) == 0 {
		return nil
	}

	lineGap := estimateBaseGap(rows)
	paragraphGap := maxInt64(18, int64(float64(lineGap)*1.8))
	blocks := make([]string, 0)
	current := ""

	flush := func() {
		current = cleanExtractedText(current)
		if current != "" {
			blocks = append(blocks, current)
		}
		current = ""
	}

	for index, row := range rows {
		line := row.Text
		if line == "" {
			continue
		}

		if current == "" {
			current = line
			continue
		}

		prev := rows[index-1]
		gap := prev.Position - row.Position
		if shouldStartNewPDFBlock(current, line, gap, paragraphGap) {
			flush()
			current = line
			continue
		}

		current = mergePDFLines(current, line)
	}

	flush()
	return blocks
}

func estimateBaseGap(rows []pdfRow) int64 {
	gaps := make([]int64, 0, len(rows))
	for i := 1; i < len(rows); i++ {
		gap := rows[i-1].Position - rows[i].Position
		if gap > 0 {
			gaps = append(gaps, gap)
		}
	}
	if len(gaps) == 0 {
		return 12
	}
	sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
	return gaps[len(gaps)/2]
}

func shouldStartNewPDFBlock(current string, next string, gap int64, paragraphGap int64) bool {
	if next == "" {
		return false
	}
	if isLikelyHeading(next) {
		return true
	}
	if isBulletLine(next) {
		return true
	}
	if gap >= paragraphGap {
		return true
	}
	if strings.HasSuffix(strings.TrimSpace(current), ":") {
		return true
	}
	return false
}

func mergePDFLines(current string, next string) string {
	current = strings.TrimRightFunc(current, unicode.IsSpace)
	next = strings.TrimLeftFunc(next, unicode.IsSpace)
	if current == "" {
		return next
	}
	if next == "" {
		return current
	}

	currentRunes := []rune(current)
	nextRunes := []rune(next)
	last := currentRunes[len(currentRunes)-1]
	first := nextRunes[0]

	if last == '-' && unicode.IsLower(first) {
		return strings.TrimRight(current[:len(current)-1], " ") + next
	}
	if shouldInsertSpaceBetween(current, next) {
		return current + " " + next
	}
	return current + next
}

func cleanExtractedText(text string) string {
	cleaned, _ := cleanExtractedTextWithStats(text)
	return cleaned
}

func cleanExtractedTextWithStats(text string) (string, textCleaningStats) {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	rawTextLength := len(strings.TrimSpace(text))

	lines := strings.Split(text, "\n")
	cleaned := make([]string, 0, len(lines))
	blankCount := 0
	for _, line := range lines {
		line = normalizePDFLine(line)
		if line == "" {
			blankCount++
			if blankCount > 1 {
				continue
			}
			cleaned = append(cleaned, "")
			continue
		}

		blankCount = 0
		cleaned = append(cleaned, line)
	}

	text = strings.Join(cleaned, "\n")
	text = extraBlankLinesPattern.ReplaceAllString(text, "\n\n")
	text = strings.TrimSpace(text)
	return text, textCleaningStats{
		RawTextLength:          rawTextLength,
		CleanedTextLength:      len(text),
		RemovedNoiseLinesCount: 0,
	}
}

func normalizePDFLine(line string) string {
	line = cleanInlineText(line)
	line = spaceBeforePunctPattern.ReplaceAllString(line, "$1")
	line = missingSpaceAfterPunctPattern.ReplaceAllString(line, "$1 $2")
	line = spaceAroundParenPattern.ReplaceAllStringFunc(line, func(match string) string {
		if strings.HasPrefix(match, "(") {
			return "("
		}
		return ")"
	})
	line = spacedSlashPattern.ReplaceAllString(line, " $1 ")
	line = repeatedDashSpacePattern.ReplaceAllString(line, " $1 ")
	line = inlineWhitespacePattern.ReplaceAllString(line, " ")
	return strings.TrimSpace(line)
}

func pdfTextQualityScore(text string) int {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0
	}
	words := strings.Fields(text)
	if len(words) == 0 {
		return 0
	}
	good := 0
	shortLower := 0
	veryLongLower := 0
	letters := 0
	totalRunes := 0
	for _, word := range words {
		word = strings.Trim(word, ".,;:!?()[]{}\"'")
		wordRunes := []rune(word)
		if len(wordRunes) >= 3 && containsLetter(word) {
			good++
		}
		if len(wordRunes) <= 2 && containsLetter(word) && isLowercaseWord(word) {
			shortLower++
		}
		if len(wordRunes) >= 18 && isLowercaseWord(word) {
			veryLongLower++
		}
		for _, r := range wordRunes {
			totalRunes++
			if unicode.IsLetter(r) {
				letters++
			}
		}
	}
	score := (good * 100) / len(words)
	score -= (shortLower * 35) / len(words)
	score -= veryLongLower * 3
	if totalRunes > 0 && letters*100/totalRunes < 55 {
		score -= 15
	}
	if score < 0 {
		return 0
	}
	if score > 100 {
		return 100
	}
	return score
}

func pdfTextPreview(text string, limit int) string {
	text = strings.Join(strings.Fields(text), " ")
	if limit <= 0 {
		return ""
	}
	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}
	return string(runes[:limit])
}

func shouldMergePDFSegments(left string, right string, gap float64) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return false
	}
	if gap > 1.1 {
		return false
	}

	leftRunes := []rune(left)
	rightRunes := []rune(right)
	last := leftRunes[len(leftRunes)-1]
	first := rightRunes[0]
	if !unicode.IsLetter(last) || !unicode.IsLetter(first) {
		return false
	}
	if !unicode.IsLower(first) {
		return false
	}

	lastToken := lastAlphabeticToken(left)
	firstToken := firstAlphabeticToken(right)
	if lastToken == "" || firstToken == "" {
		return false
	}
	return len([]rune(lastToken))+len([]rune(firstToken)) >= 5
}

func cleanInlineText(text string) string {
	replacer := strings.NewReplacer(
		"\u00a0", " ",
		"\u2007", " ",
		"\u202f", " ",
		"\u200b", "",
		"\u200c", "",
		"\u200d", "",
		"\ufeff", "",
	)
	text = replacer.Replace(text)
	text = inlineWhitespacePattern.ReplaceAllString(text, " ")
	return strings.TrimSpace(text)
}

func isLowercaseWord(word string) bool {
	hasLetter := false
	for _, r := range word {
		if !unicode.IsLetter(r) {
			continue
		}
		hasLetter = true
		if unicode.IsUpper(r) {
			return false
		}
	}
	return hasLetter
}

func fixBrokenTitleWords(line string) string {
	tokens := strings.Fields(line)
	if len(tokens) < 2 {
		return line
	}

	merged := make([]string, 0, len(tokens))
	for i := 0; i < len(tokens); i++ {
		current := tokens[i]
		if i < len(tokens)-1 {
			next := tokens[i+1]
			if shouldJoinBrokenWord(current, next) {
				current += next
				i++
			}
		}
		merged = append(merged, current)
	}

	return strings.Join(merged, " ")
}

func shouldJoinBrokenWord(left string, right string) bool {
	left = strings.Trim(left, ".,;:!?()[]{}\"'")
	right = strings.Trim(right, ".,;:!?()[]{}\"'")
	if left == "" || right == "" {
		return false
	}
	if startsUpper(left) && startsUpper(right) {
		return false
	}
	if !containsLetter(left) || !containsLetter(right) {
		return false
	}

	leftRunes := []rune(left)
	rightRunes := []rune(right)
	if !unicode.IsLetter(leftRunes[len(leftRunes)-1]) || !unicode.IsLetter(rightRunes[0]) {
		return false
	}
	if !unicode.IsLower(rightRunes[0]) {
		return false
	}
	if len(leftRunes) > 4 && len(rightRunes) > 4 {
		return false
	}

	combined := strings.ToLower(left + right)
	if len([]rune(combined)) < 5 {
		return false
	}
	if isCommonStandaloneWord(strings.ToLower(left)) || isCommonStandaloneWord(strings.ToLower(right)) {
		return false
	}
	return startsUpper(left) || len(leftRunes) <= 3 || len(rightRunes) <= 3
}

func isCommonStandaloneWord(word string) bool {
	switch word {
	case "de", "del", "la", "el", "los", "las", "un", "una", "unas", "unos", "para", "con", "sin", "por", "que", "como", "esta", "este", "estos", "estas", "entre", "sobre":
		return true
	default:
		return false
	}
}

func isAcademicNoiseLine(line string) bool {
	normalized := NormalizeSearchText(line)
	if normalized == "" {
		return false
	}
	if containsRelevantContent(line) {
		return false
	}

	conditions := 0

	if isbnPattern.MatchString(line) {
		conditions += 2
	}

	for _, term := range []string{"referencias", "fuentes", "bibliografia", "bibliografia consultada", "works cited"} {
		if strings.Contains(normalized, term) {
			conditions++
			break
		}
	}

	if looksLikeBibliographicReference(line, normalized) {
		conditions++
	}
	if looksLikeAuthorLine(line) {
		conditions++
	}
	if looksLikeBookTitleLine(line, normalized) {
		conditions++
	}

	return conditions >= 2
}

func looksLikeBibliographicReference(line string, normalized string) bool {
	score := 0
	if !yearPattern.MatchString(line) {
		if !strings.Contains(normalized, "doi") {
			return false
		}
	} else {
		score++
	}
	if strings.Count(line, ",") >= 2 {
		score++
	}
	if strings.Contains(line, "(") && strings.Contains(line, ")") {
		score++
	}
	for _, marker := range []string{"editorial", "press", "springer", "pearson", "mcgraw", "routledge", "wiley", "university", "vol.", "doi", "pp."} {
		if strings.Contains(normalized, marker) {
			score++
			break
		}
	}
	return score >= 2
}

func looksLikeAuthorLine(line string) bool {
	line = strings.TrimSpace(line)
	if strings.ContainsAny(line, ".:!?") {
		return false
	}
	parts := authorSeparatorPattern.Split(line, -1)
	if len(parts) < 1 || len(parts) > 5 {
		return false
	}

	validNames := 0
	for _, part := range parts {
		words := strings.Fields(strings.TrimSpace(part))
		if len(words) < 2 || len(words) > 4 {
			return false
		}
		nameWords := 0
		for _, word := range words {
			word = strings.Trim(word, "[](){}-")
			if word == "" || !startsUpper(word) {
				return false
			}
			nameWords++
		}
		if nameWords >= 2 {
			validNames++
		}
	}

	return validNames > 0
}

func looksLikeBookTitleLine(line string, normalized string) bool {
	words := strings.Fields(line)
	if len(words) < 2 || len(words) > 10 {
		return false
	}
	if strings.ContainsAny(line, ".!?") {
		return false
	}
	if !mostlyTitleCase(words) {
		return false
	}
	for _, marker := range []string{"manual", "handbook", "textbook", "introduction", "fundamentos", "principios", "teoria", "theory", "course", "guide"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func mostlyTitleCase(words []string) bool {
	titleWords := 0
	for _, word := range words {
		word = strings.Trim(word, "[](){}:;,-")
		if word == "" {
			continue
		}
		if startsUpper(word) || word == strings.ToUpper(word) {
			titleWords++
		}
	}
	return titleWords >= len(words)-1
}

func shouldDropShortIrrelevantLine(line string) bool {
	if isLikelyHeading(line) || isBulletLine(line) || looksLikeSectionNumber(strings.Fields(line)[0]) {
		return false
	}
	words := strings.Fields(line)
	if len(words) > 3 {
		return false
	}
	if containsRelevantContent(line) {
		return false
	}
	for _, word := range words {
		if len([]rune(word)) >= 6 {
			return false
		}
	}
	return true
}

func normalizeHeadingCapitalization(line string) string {
	words := strings.Fields(strings.ToLower(strings.TrimSpace(line)))
	if len(words) == 0 {
		return line
	}

	for i, word := range words {
		trimmed := strings.Trim(word, "[](){}:;,-")
		if trimmed == "" {
			continue
		}
		if i > 0 && i < len(words)-1 && isLowercaseTitleStopWord(trimmed) {
			continue
		}
		words[i] = rebuildWordWithTitleCase(word)
	}

	return strings.Join(words, " ")
}

func isLowercaseTitleStopWord(word string) bool {
	switch word {
	case "de", "del", "la", "las", "el", "los", "y", "o", "en", "para", "por", "con", "sin":
		return true
	default:
		return false
	}
}

func rebuildWordWithTitleCase(word string) string {
	runes := []rune(word)
	for i, r := range runes {
		if unicode.IsLetter(r) {
			runes[i] = unicode.ToUpper(r)
			break
		}
	}
	return string(runes)
}

func lastAlphabeticToken(text string) string {
	words := strings.Fields(text)
	for i := len(words) - 1; i >= 0; i-- {
		word := strings.Trim(words[i], ".,;:!?()[]{}\"'")
		if containsLetter(word) {
			return word
		}
	}
	return ""
}

func firstAlphabeticToken(text string) string {
	words := strings.Fields(text)
	for _, word := range words {
		word = strings.Trim(word, ".,;:!?()[]{}\"'")
		if containsLetter(word) {
			return word
		}
	}
	return ""
}

func normalizeRepeatedLineKey(line string) string {
	line = strings.TrimSpace(line)
	if line == "" || len([]rune(line)) > 160 {
		return ""
	}
	key := strings.ToLower(line)
	key = headerFooterDigitsPattern.ReplaceAllString(key, "#")
	key = strings.Trim(key, " -_|,.;:()[]{}")
	key = inlineWhitespacePattern.ReplaceAllString(key, " ")
	if len(strings.Fields(key)) == 0 {
		return ""
	}
	return key
}

func shouldInsertSpaceBetween(left string, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return false
	}

	leftRunes := []rune(left)
	rightRunes := []rune(right)
	last := leftRunes[len(leftRunes)-1]
	first := rightRunes[0]

	if unicode.IsSpace(last) || unicode.IsSpace(first) {
		return false
	}
	if strings.ContainsRune("([{/@", last) {
		return false
	}
	if strings.ContainsRune(".,;:!?)]}/%", first) {
		return false
	}
	if last == '-' {
		return false
	}
	if unicode.IsLetter(last) && unicode.IsLetter(first) {
		return true
	}
	if unicode.IsDigit(last) && unicode.IsDigit(first) {
		return true
	}
	if unicode.IsLetter(last) && unicode.IsDigit(first) {
		return true
	}
	if unicode.IsDigit(last) && unicode.IsLetter(first) {
		return true
	}
	return false
}

func shouldInsertSpaceBetweenPDFSegments(left string, right string, gap float64) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return false
	}

	leftRunes := []rune(left)
	rightRunes := []rune(right)
	last := leftRunes[len(leftRunes)-1]
	first := rightRunes[0]

	if unicode.IsSpace(last) || unicode.IsSpace(first) {
		return false
	}
	if strings.ContainsRune("([{/@", last) {
		return false
	}
	if strings.ContainsRune(".,;:!?)]}/%", first) {
		return false
	}
	if last == '-' {
		return false
	}
	if unicode.IsLetter(last) && unicode.IsLetter(first) {
		return gap >= 3.5
	}
	if unicode.IsDigit(last) && unicode.IsDigit(first) {
		return gap >= 2.2
	}
	if unicode.IsLetter(last) && unicode.IsDigit(first) {
		return gap >= 2.2
	}
	if unicode.IsDigit(last) && unicode.IsLetter(first) {
		return gap >= 2.2
	}
	return gap >= 2.2
}

func isLikelyHeading(text string) bool {
	text = strings.TrimSpace(text)
	if text == "" {
		return false
	}

	runes := []rune(text)
	if len(runes) > 120 {
		return false
	}
	if strings.HasSuffix(text, ".") || strings.HasSuffix(text, ",") || strings.HasSuffix(text, ";") {
		return false
	}
	if isBulletLine(text) {
		return false
	}

	words := strings.Fields(text)
	if len(words) == 0 || len(words) > 12 {
		return false
	}

	if looksLikeSectionNumber(words[0]) {
		return true
	}

	upperWords := 0
	titleWords := 0
	for _, word := range words {
		word = strings.Trim(word, "[](){}:.-")
		if word == "" {
			continue
		}
		if word == strings.ToUpper(word) && containsLetter(word) {
			upperWords++
		}
		if startsUpper(word) {
			titleWords++
		}
	}

	return upperWords == len(words) || (len(words) <= 8 && titleWords >= len(words)-1)
}

func isBulletLine(text string) bool {
	text = strings.TrimSpace(text)
	if text == "" {
		return false
	}
	for _, prefix := range []string{"- ", "* ", "• ", "· "} {
		if strings.HasPrefix(text, prefix) {
			return true
		}
	}

	fields := strings.Fields(text)
	if len(fields) == 0 {
		return false
	}
	first := strings.Trim(fields[0], ")].")
	if _, err := strconv.Atoi(first); err == nil {
		return true
	}
	if len(fields[0]) == 2 && unicode.IsLetter([]rune(fields[0])[0]) && strings.HasSuffix(fields[0], ")") {
		return true
	}
	return false
}

func looksLikeSectionNumber(token string) bool {
	token = strings.TrimSpace(token)
	token = strings.Trim(token, ".)")
	if token == "" {
		return false
	}
	parts := strings.Split(token, ".")
	if len(parts) > 4 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			continue
		}
		if _, err := strconv.Atoi(part); err != nil {
			return false
		}
	}
	return true
}

func containsLetter(text string) bool {
	for _, r := range text {
		if unicode.IsLetter(r) {
			return true
		}
	}
	return false
}

func startsUpper(text string) bool {
	for _, r := range text {
		if unicode.IsLetter(r) {
			return unicode.IsUpper(r)
		}
	}
	return false
}

func containsRelevantContent(line string) bool {
	normalized := NormalizeSearchText(line)
	if normalized == "" {
		return false
	}
	if len(strings.Fields(normalized)) > 3 {
		return true
	}
	for _, term := range []string{
		"programacion",
		"programa",
		"algoritmo",
		"algoritmos",
		"arreglo",
		"arreglos",
		"estructura",
		"estructuras",
		"funcion",
		"funciones",
		"variable",
		"variables",
		"dato",
		"datos",
		"sistema",
		"proceso",
		"procesos",
		"modelo",
		"metodo",
		"metodos",
		"clase",
		"clases",
		"objeto",
		"objetos",
	} {
		if strings.Contains(normalized, term) {
			return true
		}
	}
	return false
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func maxInt64(a int64, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
