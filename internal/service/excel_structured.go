package service

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/xuri/excelize/v2"
)

type ExcelDocument struct {
	Sheets        []ExcelSheet
	RowsProcessed int
}

type ExcelSheet struct {
	Name           string
	Headers        []string
	HeaderDetected bool
	Rows           []ExcelRow
	DataRowCount   int
}

type ExcelRow struct {
	Number          int
	SourceRowNumber int
	Cells           []string
}

type ExcelChunkStats struct {
	SheetCount      int
	SheetNames      []string
	RowsProcessed   int
	ChunksGenerated int
}

func parseStructuredXLSX(data []byte) (*ExcelDocument, string, error) {
	if len(data) == 0 {
		return nil, "", fmt.Errorf("xlsx vacio: sin bytes para leer")
	}

	file, err := excelize.OpenReader(bytes.NewReader(data))
	if err != nil {
		return nil, "", fmt.Errorf("error abriendo xlsx con excelize: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()

	sheetNames := file.GetSheetList()
	if len(sheetNames) == 0 {
		return nil, "", fmt.Errorf("xlsx sin hojas")
	}

	document := &ExcelDocument{
		Sheets: make([]ExcelSheet, 0, len(sheetNames)),
	}

	for _, sheetName := range sheetNames {
		rows, err := file.GetRows(sheetName)
		if err != nil {
			return nil, "", fmt.Errorf("error leyendo hoja %s: %w", sheetName, err)
		}

		parsedSheet, err := buildStructuredExcelSheet(sheetName, rows)
		if err != nil {
			return nil, "", err
		}
		if parsedSheet == nil {
			continue
		}

		document.Sheets = append(document.Sheets, *parsedSheet)
		document.RowsProcessed += parsedSheet.DataRowCount
	}

	if len(document.Sheets) == 0 {
		return nil, "", fmt.Errorf("xlsx sin datos legibles: todas las hojas estan vacias")
	}

	content := buildExcelStructuredText(document)
	if strings.TrimSpace(content) == "" {
		return nil, "", fmt.Errorf("xlsx sin texto estructurado util tras el procesamiento")
	}

	return document, content, nil
}

func SplitExcelIntoChunks(doc *ExcelDocument, maxLen int) ([]string, ExcelChunkStats) {
	if doc == nil || len(doc.Sheets) == 0 {
		return nil, ExcelChunkStats{}
	}
	if maxLen <= 0 {
		maxLen = 500
	}

	stats := ExcelChunkStats{
		SheetCount:    len(doc.Sheets),
		RowsProcessed: doc.RowsProcessed,
		SheetNames:    make([]string, 0, len(doc.Sheets)),
	}
	chunks := make([]string, 0)

	for _, sheet := range doc.Sheets {
		stats.SheetNames = append(stats.SheetNames, sheet.Name)
		baseLines := []string{
			"Documento tipo: Excel",
			"Hoja: " + sheet.Name,
			"Columnas: " + strings.Join(sheet.Headers, ", "),
			fmt.Sprintf("Total de columnas: %d", len(sheet.Headers)),
			fmt.Sprintf("Total de filas de datos: %d", sheet.DataRowCount),
		}
		if sheet.HeaderDetected {
			baseLines = append(baseLines, "Encabezados detectados: si")
		} else {
			baseLines = append(baseLines, "Encabezados detectados: no")
		}

		if sheet.DataRowCount == 0 {
			chunk := cleanExtractedText(strings.Join(baseLines, "\n"))
			if chunk != "" {
				chunks = append(chunks, chunk)
			}
			continue
		}

		currentRows := make([]ExcelRow, 0)
		for _, row := range sheet.Rows {
			candidateRows := append(currentRows, row)
			candidateChunk := buildExcelChunk(baseLines, sheet, candidateRows)
			if len(currentRows) > 0 && chunkLen(candidateChunk) > maxLen {
				chunks = append(chunks, buildExcelChunk(baseLines, sheet, currentRows))
				currentRows = []ExcelRow{row}
				continue
			}
			currentRows = candidateRows
		}

		if len(currentRows) > 0 {
			chunks = append(chunks, buildExcelChunk(baseLines, sheet, currentRows))
		}
	}

	cleanedChunks := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		chunk = cleanExtractedText(chunk)
		if chunk == "" {
			continue
		}
		if chunkLen(chunk) <= maxLen {
			cleanedChunks = append(cleanedChunks, chunk)
			continue
		}
		cleanedChunks = append(cleanedChunks, splitLargeBlock(chunk, maxLen)...)
	}

	stats.ChunksGenerated = len(cleanedChunks)
	return cleanedChunks, stats
}

func buildStructuredExcelSheet(sheetName string, rows [][]string) (*ExcelSheet, error) {
	rawRows := make([][]string, 0, len(rows))
	sourceRowNumbers := make([]int, 0, len(rows))
	for rowIndex, row := range rows {
		trimmed := normalizeExcelRow(row)
		if isExcelRowEmpty(trimmed) {
			continue
		}
		rawRows = append(rawRows, trimmed)
		sourceRowNumbers = append(sourceRowNumbers, rowIndex+1)
	}

	if len(rawRows) == 0 {
		return nil, nil
	}

	headers, headerDetected, dataStart := detectExcelHeaders(rawRows)
	if len(headers) == 0 {
		headers = generatedExcelHeaders(maxColumnsInRows(rawRows))
	}

	sheet := &ExcelSheet{
		Name:           strings.TrimSpace(sheetName),
		Headers:        headers,
		HeaderDetected: headerDetected,
		Rows:           make([]ExcelRow, 0, max(0, len(rawRows)-dataStart)),
	}

	dataRowNumber := 0
	for i := dataStart; i < len(rawRows); i++ {
		cells := fillExcelRowToHeaders(rawRows[i], len(headers))
		if isExcelRowEmpty(cells) {
			continue
		}
		dataRowNumber++
		sheet.Rows = append(sheet.Rows, ExcelRow{
			Number:          dataRowNumber,
			SourceRowNumber: sourceRowNumbers[i],
			Cells:           cells,
		})
	}

	sheet.DataRowCount = len(sheet.Rows)
	return sheet, nil
}

func buildExcelStructuredText(doc *ExcelDocument) string {
	if doc == nil || len(doc.Sheets) == 0 {
		return ""
	}

	var builder strings.Builder
	for _, sheet := range doc.Sheets {
		if builder.Len() > 0 {
			builder.WriteString("\n\n")
		}
		builder.WriteString("Hoja: ")
		builder.WriteString(sheet.Name)
		builder.WriteString("\n")
		builder.WriteString("Columnas: ")
		builder.WriteString(strings.Join(sheet.Headers, ", "))
		builder.WriteString("\n")
		builder.WriteString(fmt.Sprintf("Total de columnas: %d\n", len(sheet.Headers)))
		builder.WriteString(fmt.Sprintf("Total de filas de datos: %d\n", sheet.DataRowCount))
		if sheet.HeaderDetected {
			builder.WriteString("Encabezados detectados: si\n")
		} else {
			builder.WriteString("Encabezados detectados: no\n")
		}
		for _, row := range sheet.Rows {
			line := formatExcelRow(sheet.Headers, row)
			if line == "" {
				continue
			}
			builder.WriteString(line)
			builder.WriteString("\n")
		}
	}
	return cleanExtractedText(builder.String())
}

func buildExcelChunk(baseLines []string, sheet ExcelSheet, rows []ExcelRow) string {
	lines := make([]string, 0, len(baseLines)+len(rows)+1)
	lines = append(lines, baseLines...)
	if len(rows) > 0 {
		lines = append(lines, fmt.Sprintf("Bloque de filas: %d-%d", rows[0].Number, rows[len(rows)-1].Number))
	}
	for _, row := range rows {
		line := formatExcelRow(sheet.Headers, row)
		if line != "" {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "\n")
}

func formatExcelRow(headers []string, row ExcelRow) string {
	pairs := make([]string, 0, len(row.Cells))
	for i, value := range row.Cells {
		value = strings.TrimSpace(cleanExtractedText(value))
		if value == "" {
			continue
		}
		header := columnNameForIndex(i)
		if i < len(headers) && strings.TrimSpace(headers[i]) != "" {
			header = headers[i]
		}
		pairs = append(pairs, header+"="+value)
	}
	if len(pairs) == 0 {
		return ""
	}
	return fmt.Sprintf("Fila %d: %s", row.Number, strings.Join(pairs, " | "))
}

func normalizeExcelRow(row []string) []string {
	if len(row) == 0 {
		return nil
	}
	normalized := make([]string, 0, len(row))
	for _, cell := range row {
		normalized = append(normalized, strings.TrimSpace(cleanExtractedText(cell)))
	}
	return trimTrailingEmptyCells(normalized)
}

func fillExcelRowToHeaders(row []string, headersLen int) []string {
	if headersLen <= 0 {
		return trimTrailingEmptyCells(row)
	}
	filled := make([]string, headersLen)
	copy(filled, row)
	return trimTrailingEmptyCells(filled)
}

func detectExcelHeaders(rows [][]string) ([]string, bool, int) {
	if len(rows) == 0 {
		return nil, false, 0
	}

	first := trimTrailingEmptyCells(rows[0])
	if len(first) == 0 {
		return nil, false, 0
	}
	if len(rows) == 1 {
		return sanitizeExcelHeaders(first), true, 1
	}

	second := trimTrailingEmptyCells(rows[1])
	firstLooksHeader := excelRowLooksLikeHeader(first)
	secondLooksHeader := excelRowLooksLikeHeader(second)
	secondLooksData := excelRowLooksLikeData(second)

	if firstLooksHeader && (secondLooksData || !secondLooksHeader) {
		return sanitizeExcelHeaders(first), true, 1
	}

	return generatedExcelHeaders(maxColumnsInRows(rows)), false, 0
}

func excelRowLooksLikeHeader(row []string) bool {
	if len(row) == 0 {
		return false
	}

	seen := make(map[string]bool, len(row))
	textCells := 0
	nonEmpty := 0
	for _, cell := range row {
		cell = strings.TrimSpace(cell)
		if cell == "" {
			continue
		}
		nonEmpty++
		key := NormalizeSearchText(cell)
		if key == "" || seen[key] {
			return false
		}
		seen[key] = true
		if excelCellLooksLikeText(cell) && !excelCellLooksNumeric(cell) {
			textCells++
		}
	}
	if nonEmpty == 0 {
		return false
	}
	return textCells >= max(1, nonEmpty-1)
}

func excelRowLooksLikeData(row []string) bool {
	if len(row) == 0 {
		return false
	}

	nonEmpty := 0
	numericLike := 0
	for _, cell := range row {
		cell = strings.TrimSpace(cell)
		if cell == "" {
			continue
		}
		nonEmpty++
		if excelCellLooksNumeric(cell) || strings.Contains(cell, "/") || strings.Contains(cell, "-") {
			numericLike++
		}
	}
	if nonEmpty == 0 {
		return false
	}
	return numericLike > 0
}

func excelCellLooksLikeText(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}

	letters := 0
	digits := 0
	for _, r := range value {
		switch {
		case unicode.IsLetter(r):
			letters++
		case unicode.IsDigit(r):
			digits++
		}
	}
	return letters > 0 && letters >= digits
}

func excelCellLooksNumeric(value string) bool {
	value = strings.TrimSpace(strings.ReplaceAll(value, ",", "."))
	if value == "" {
		return false
	}
	_, err := strconv.ParseFloat(value, 64)
	return err == nil
}

func sanitizeExcelHeaders(row []string) []string {
	headers := make([]string, 0, len(row))
	for i, cell := range row {
		cell = strings.TrimSpace(cleanExtractedText(cell))
		if cell == "" {
			headers = append(headers, columnNameForIndex(i))
			continue
		}
		headers = append(headers, cell)
	}
	return trimTrailingEmptyCells(headers)
}

func generatedExcelHeaders(count int) []string {
	if count <= 0 {
		return nil
	}
	headers := make([]string, 0, count)
	for i := 0; i < count; i++ {
		headers = append(headers, columnNameForIndex(i))
	}
	return headers
}

func columnNameForIndex(index int) string {
	if index < 0 {
		return "Columna"
	}
	return "Columna " + xlsxColumnLabel(index)
}

func xlsxColumnLabel(index int) string {
	index++
	label := ""
	for index > 0 {
		index--
		label = string(rune('A'+(index%26))) + label
		index /= 26
	}
	return label
}

func trimTrailingEmptyCells(values []string) []string {
	last := len(values) - 1
	for last >= 0 {
		if strings.TrimSpace(values[last]) != "" {
			break
		}
		last--
	}
	if last < 0 {
		return nil
	}
	trimmed := make([]string, last+1)
	copy(trimmed, values[:last+1])
	return trimmed
}

func isExcelRowEmpty(values []string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return false
		}
	}
	return true
}

func maxColumnsInRows(rows [][]string) int {
	maxColumns := 0
	for _, row := range rows {
		if len(row) > maxColumns {
			maxColumns = len(row)
		}
	}
	return maxColumns
}
