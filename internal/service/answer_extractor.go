package service

import (
	"log"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"auradb-pipeline/internal/repository/postgres"
)

var (
	numericUnitPattern         = regexp.MustCompile(`\b\d+(?:[.,]\d+)?\s*(?:%|por ciento|mes(?:es)?|dia(?:s)?|dias|ano(?:s)?|anos|hora(?:s)?|minuto(?:s)?|segundo(?:s)?|km|kilometro(?:s)?|kilometros|m|metro(?:s)?|metros)\b`)
	dateLikePattern            = regexp.MustCompile(`\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b|\b\d{4}\b`)
	personNamePattern          = regexp.MustCompile(`\b[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,3}\b`)
	whitespaceJoiner           = regexp.MustCompile(`\s+`)
	leadingNoiseHeadingPattern = regexp.MustCompile(`(?i)^(metodologia|metodología|introduccion|introducción|conclusion|conclusión|resumen|resultado|objetivo)\s*[,:\-]*\s*`)
	leadingSymbolPattern       = regexp.MustCompile(`^[,;:.\-\s¿?¡!]+`)
	joinedOCRPhrasesPattern    = []ocrReplacement{
		{old: "duraciónde", new: "duración de"},
		{old: "duracionde", new: "duracion de"},
		{old: "deentre", new: "de entre"},
		{old: "elviaje", new: "el viaje"},
		{old: "viajetiene", new: "viaje tiene"},
		{old: "nuevemeses", new: "nueve meses"},
		{old: "seismeses", new: "seis meses"},
		{old: "sietemeses", new: "siete meses"},
		{old: "ochomeses", new: "ocho meses"},
		{old: "lavia", new: "la via"},
		{old: "seisynueve", new: "seis y nueve"},
		{old: "sieteyocho", new: "siete y ocho"},
		{old: "ochoynueve", new: "ocho y nueve"},
		{old: "unoydos", new: "uno y dos"},
		{old: "dosytres", new: "dos y tres"},
		{old: "tresycuatro", new: "tres y cuatro"},
		{old: "cuatroycinco", new: "cuatro y cinco"},
		{old: "cincoyseis", new: "cinco y seis"},
		{old: "horasde", new: "horas de"},
		{old: "mesesde", new: "meses de"},
		{old: "añosde", new: "años de"},
		{old: "anosde", new: "anos de"},
	}
)

type ocrReplacement struct {
	old string
	new string
}

type extractiveCandidate struct {
	Sentence string
	Score    int
}

func TryExtractAnswer(question string, chunks []postgres.SearchResult) (string, string, bool, bool) {
	if len(chunks) == 0 {
		return "", "none", false, false
	}

	question = NormalizeQuestionForRetrieval(question)
	normalizedQuestion := NormalizeSearchText(question)
	if normalizedQuestion == "" {
		return "", "none", false, false
	}

	if answer, cleanupApplied, ok := tryExtractExcelColumns(normalizedQuestion, chunks); ok {
		return answer, "excel_columns", cleanupApplied, true
	}
	if answer, cleanupApplied, ok := tryExtractExcelCount(normalizedQuestion, chunks); ok {
		return answer, "excel_count", cleanupApplied, true
	}
	if answer, cleanupApplied, ok := tryExtractExcelFieldValue(normalizedQuestion, chunks); ok {
		return answer, "excel_field_value", cleanupApplied, true
	}
	if answer, cleanupApplied, ok := tryExtractComparison(normalizedQuestion, chunks); ok {
		return answer, "comparison", cleanupApplied, true
	}
	if answer, cleanupApplied, ok := tryExtractDefinition(normalizedQuestion, chunks); ok {
		return answer, "definition", cleanupApplied, true
	}
	if answer, cleanupApplied, ok := tryExtractPerson(normalizedQuestion, chunks); ok {
		return answer, "person", cleanupApplied, true
	}
	if answer, cleanupApplied, ok := tryExtractDate(normalizedQuestion, chunks); ok {
		return answer, "date", cleanupApplied, true
	}
	if answer, cleanupApplied, ok := tryExtractQuantity(normalizedQuestion, chunks); ok {
		return answer, "quantity", cleanupApplied, true
	}

	return "", "none", false, false
}

func tryExtractExcelColumns(question string, chunks []postgres.SearchResult) (string, bool, bool) {
	if !strings.Contains(question, "columna") {
		return "", false, false
	}

	for _, chunk := range chunks {
		for _, line := range strings.Split(chunk.Content, "\n") {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, "Columnas:") {
				continue
			}
			columns := strings.TrimSpace(strings.TrimPrefix(line, "Columnas:"))
			if columns == "" {
				continue
			}
			answer := "Columnas: " + columns
			return answer, false, true
		}
	}

	return "", false, false
}

func tryExtractExcelCount(question string, chunks []postgres.SearchResult) (string, bool, bool) {
	if !containsAny(question, "registro", "registros", "fila", "filas") {
		return "", false, false
	}

	for _, chunk := range chunks {
		for _, line := range strings.Split(chunk.Content, "\n") {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, "Total de filas de datos:") {
				continue
			}
			total := strings.TrimSpace(strings.TrimPrefix(line, "Total de filas de datos:"))
			if total == "" {
				continue
			}
			return "Total de registros: " + total, false, true
		}
	}

	count := 0
	for _, chunk := range chunks {
		for _, line := range strings.Split(chunk.Content, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "Fila ") {
				count++
			}
		}
	}
	if count > 0 {
		return "Total de registros visibles en el contexto: " + strconv.Itoa(count), false, true
	}

	return "", false, false
}

func tryExtractExcelFieldValue(question string, chunks []postgres.SearchResult) (string, bool, bool) {
	field, entity := excelFieldQueryParts(question)
	if field == "" || entity == "" {
		return "", false, false
	}

	for _, chunk := range chunks {
		for _, line := range strings.Split(chunk.Content, "\n") {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, "Fila ") {
				continue
			}

			values := parseExcelRowPairs(line)
			if len(values) == 0 || !excelRowContainsEntity(values, entity) {
				continue
			}

			if value, ok := excelMatchField(values, field); ok {
				return value, false, true
			}
		}
	}

	return "", false, false
}

func tryExtractComparison(question string, chunks []postgres.SearchResult) (string, bool, bool) {
	if !strings.Contains(question, "diferencia entre ") {
		return "", false, false
	}

	x, y := comparisonTerms(question)
	if x == "" || y == "" {
		return "", false, false
	}

	bestCombined := ""
	bestX := ""
	bestY := ""
	for _, chunk := range chunks {
		sentences := chunkSentences(chunk.Content)
		for _, sentence := range sentences {
			normalizedSentence := NormalizeSearchText(sentence)
			if normalizedSentence == "" {
				continue
			}
			if strings.Contains(normalizedSentence, x) && strings.Contains(normalizedSentence, y) {
				bestCombined = sentence
				break
			}
			if bestX == "" && strings.Contains(normalizedSentence, x) {
				bestX = sentence
			}
			if bestY == "" && strings.Contains(normalizedSentence, y) {
				bestY = sentence
			}
		}
		if bestCombined != "" {
			break
		}
	}

	if bestCombined != "" {
		cleaned, cleanupApplied := cleanExtractiveSentence(bestCombined)
		return "Según el documento, " + cleaned, cleanupApplied, true
	}
	if bestX != "" && bestY != "" {
		cleanedX, cleanupAppliedX := cleanExtractiveSentence(bestX)
		cleanedY, cleanupAppliedY := cleanExtractiveSentence(bestY)
		return "Según el documento, " + cleanedX + " mientras que " + cleanedY, cleanupAppliedX || cleanupAppliedY, true
	}
	return "", false, false
}

func tryExtractDefinition(question string, chunks []postgres.SearchResult) (string, bool, bool) {
	if !isDefinitionQuestion(question) {
		return "", false, false
	}

	subject := cleanEntityTail(question, []string{"que es ", "qué es ", "que significa ", "qué significa ", "define ", "definicion de ", "definición de "})
	intent := detectQueryIntent(subject)
	bestSentence := ""
	bestScore := -1
	for _, chunk := range chunks {
		for _, sentence := range chunkSentences(chunk.Content) {
			score := definitionSentenceScore(intent, sentence)
			if score > bestScore {
				bestScore = score
				bestSentence = sentence
			}
		}
	}
	if bestScore < 2 {
		return "", false, false
	}
	cleaned, cleanupApplied := cleanExtractiveSentence(bestSentence)
	return cleaned, cleanupApplied, true
}

func tryExtractPerson(question string, chunks []postgres.SearchResult) (string, bool, bool) {
	if !strings.Contains(question, "quien") && !strings.Contains(question, "quién") {
		return "", false, false
	}

	intent := detectQueryIntent(question)
	bestSentence := ""
	bestScore := -1
	for _, chunk := range chunks {
		for _, sentence := range chunkSentences(chunk.Content) {
			score := sentenceMatchScore(intent, sentence)
			if personNamePattern.MatchString(sentence) {
				score += 3
			}
			normalizedSentence := NormalizeSearchText(sentence)
			for _, marker := range []string{"fue ", "es ", "era ", "por ", "lider", "autor", "responsable", "dirigido por", "presentado por"} {
				if strings.Contains(normalizedSentence, marker) {
					score++
					break
				}
			}
			if score > bestScore {
				bestScore = score
				bestSentence = sentence
			}
		}
	}
	if bestScore < 3 {
		return "", false, false
	}
	cleaned, cleanupApplied := cleanExtractiveSentence(bestSentence)
	return cleaned, cleanupApplied, true
}

func tryExtractDate(question string, chunks []postgres.SearchResult) (string, bool, bool) {
	if !isDateQuestion(question) {
		return "", false, false
	}

	intent := detectQueryIntent(question)
	bestSentence, candidateCount, rejectedInterrogatives := bestDeclarativeNumericSentence(intent, chunks, dateLikePattern, []string{"tiene", "dura", "ocurre", "requiere", "alcanza", "es", "tarda", "fue", "sera", "será"})
	log.Printf("extractive_sentence_candidates=%d rejected_interrogative_sentence_count=%d extractor_type=%q", candidateCount, rejectedInterrogatives, "date")
	if bestSentence == "" {
		return "", false, false
	}

	cleaned, cleanupApplied := cleanExtractiveSentence(extractDateOrQuantitySpan(bestSentence, dateLikePattern))
	return cleaned, cleanupApplied, true
}

func tryExtractQuantity(question string, chunks []postgres.SearchResult) (string, bool, bool) {
	if !isQuantityQuestion(question) {
		return "", false, false
	}

	intent := detectQueryIntent(question)
	bestSentence, candidateCount, rejectedInterrogatives := bestDeclarativeNumericSentence(intent, chunks, numericUnitPattern, []string{"tiene", "dura", "ocurre", "requiere", "alcanza", "es", "tarda"})
	log.Printf("extractive_sentence_candidates=%d rejected_interrogative_sentence_count=%d extractor_type=%q", candidateCount, rejectedInterrogatives, "quantity")
	if bestSentence == "" {
		return "", false, false
	}

	cleaned, cleanupApplied := cleanExtractiveSentence(extractDateOrQuantitySpan(bestSentence, numericUnitPattern))
	return cleaned, cleanupApplied, true
}

func bestDeclarativeNumericSentence(intent queryIntent, chunks []postgres.SearchResult, pattern *regexp.Regexp, declarativeVerbs []string) (string, int, int) {
	best := extractiveCandidate{Score: -1}
	candidateCount := 0
	rejectedInterrogatives := 0

	for _, chunk := range chunks {
		for _, sentence := range chunkSentences(chunk.Content) {
			if isInterrogativeSentence(sentence) {
				rejectedInterrogatives++
				continue
			}

			repaired := applyOCRSpacingRepairs(sentence)
			normalizedSentence := NormalizeSearchText(repaired)
			if !pattern.MatchString(normalizedSentence) {
				continue
			}

			score := sentenceMatchScore(intent, repaired) + 3
			if containsDeclarativeVerb(normalizedSentence, declarativeVerbs) {
				score += 3
			}
			if strings.Contains(normalizedSentence, " entre ") {
				score++
			}
			if strings.Contains(normalizedSentence, " favorable") || strings.Contains(normalizedSentence, " ventana") {
				score++
			}

			candidateCount++
			if score > best.Score {
				best = extractiveCandidate{
					Sentence: repaired,
					Score:    score,
				}
			}
		}
	}

	return best.Sentence, candidateCount, rejectedInterrogatives
}

func definitionSentenceScore(intent queryIntent, sentence string) int {
	normalizedSentence := NormalizeSearchText(sentence)
	score := sentenceMatchScore(intent, sentence)
	for _, marker := range []string{" es ", " se define ", " consiste en ", " se refiere a ", " significa ", " son "} {
		if strings.Contains(" "+normalizedSentence+" ", marker) {
			score += 2
			break
		}
	}
	return score
}

func sentenceMatchScore(intent queryIntent, sentence string) int {
	normalizedSentence := NormalizeSearchText(sentence)
	score := 0
	if intent.MainEntity != "" && strings.Contains(normalizedSentence, intent.MainEntity) {
		score += 4
	}
	if intent.MainPhrase != "" && strings.Contains(normalizedSentence, intent.MainPhrase) {
		score += 3
	}
	for _, term := range intent.MainTerms {
		if strings.Contains(normalizedSentence, term) {
			score++
		}
	}
	return score
}

func isInterrogativeSentence(sentence string) bool {
	trimmed := strings.TrimSpace(sentence)
	normalized := NormalizeSearchText(trimmed)
	if trimmed == "" || normalized == "" {
		return false
	}
	if strings.HasSuffix(trimmed, "?") || strings.HasPrefix(trimmed, "¿") {
		return true
	}
	for _, prefix := range []string{"que ", "qué ", "cuando ", "cuándo ", "cuanto ", "cuánto ", "cada "} {
		if strings.HasPrefix(normalized, prefix) {
			return true
		}
	}
	return false
}

func containsDeclarativeVerb(normalizedSentence string, verbs []string) bool {
	for _, verb := range verbs {
		if strings.Contains(" "+normalizedSentence+" ", " "+verb+" ") {
			return true
		}
	}
	return false
}

func comparisonTerms(question string) (string, string) {
	idx := strings.Index(question, "diferencia entre ")
	if idx < 0 {
		return "", ""
	}
	tail := strings.TrimSpace(question[idx+len("diferencia entre "):])
	parts := strings.SplitN(tail, " y ", 2)
	if len(parts) != 2 {
		return "", ""
	}
	left := cleanComparisonSide(parts[0])
	right := cleanComparisonSide(parts[1])
	return left, right
}

func cleanComparisonSide(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "¿?.,;: ")
	return NormalizeSearchText(NormalizeQuestionForRetrieval(value))
}

func isDefinitionQuestion(question string) bool {
	for _, prefix := range []string{"que es ", "qué es ", "que significa ", "qué significa ", "define ", "definicion de ", "definición de "} {
		if strings.HasPrefix(question, prefix) {
			return true
		}
	}
	return false
}

func isDateQuestion(question string) bool {
	for _, term := range []string{"cuando", "cuándo", "fecha", "ano", "año", "anos", "años", "mes", "meses", "dia", "días", "dias", "duracion", "duración", "cuanto tiempo"} {
		if strings.Contains(question, term) {
			return true
		}
	}
	return false
}

func isQuantityQuestion(question string) bool {
	for _, term := range []string{"cuanto", "cuánto", "cuantos", "cuántos", "cantidad", "porcentaje", "%", "km", "horas", "meses", "dias", "años", "anos", "registros", "registro", "filas", "fila", "columnas", "columna"} {
		if strings.Contains(question, term) {
			return true
		}
	}
	return false
}

func cleanEntityTail(question string, prefixes []string) string {
	for _, prefix := range prefixes {
		if strings.HasPrefix(question, prefix) {
			return strings.TrimSpace(question[len(prefix):])
		}
	}
	return question
}

func chunkSentences(content string) []string {
	raw := strings.ReplaceAll(content, "\r", "\n")
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == '\n' || r == '.' || r == '?' || r == '!' || r == ';'
	})
	sentences := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		sentences = append(sentences, part)
	}
	if len(sentences) == 0 && strings.TrimSpace(content) != "" {
		return []string{strings.TrimSpace(content)}
	}
	return sentences
}

func cleanExtractiveSentence(sentence string) (string, bool) {
	original := strings.TrimSpace(sentence)
	sentence = original
	sentence = leadingSymbolPattern.ReplaceAllString(sentence, "")
	sentence = leadingNoiseHeadingPattern.ReplaceAllString(sentence, "")
	sentence = whitespaceJoiner.ReplaceAllString(sentence, " ")
	sentence = strings.ReplaceAll(sentence, ",", ", ")
	sentence = whitespaceJoiner.ReplaceAllString(sentence, " ")
	sentence = applyOCRSpacingRepairs(sentence)
	sentence = leadingSymbolPattern.ReplaceAllString(sentence, "")
	sentence = leadingNoiseHeadingPattern.ReplaceAllString(sentence, "")
	sentence = strings.Trim(sentence, " .,;:¿?¡!")
	if sentence == "" {
		return "", original != sentence
	}
	runes := []rune(sentence)
	runes[0] = unicode.ToUpper(runes[0])
	sentence = string(runes)
	return sentence, original != sentence
}

func applyOCRSpacingRepairs(sentence string) string {
	repaired := sentence
	for _, replacement := range joinedOCRPhrasesPattern {
		repaired = strings.ReplaceAll(repaired, replacement.old, replacement.new)
		repaired = strings.ReplaceAll(repaired, strings.Title(replacement.old), strings.Title(replacement.new))
	}
	repaired = repairJoinedNumberWords(repaired)
	repaired = repairJoinedCommonWords(repaired)
	repaired = whitespaceJoiner.ReplaceAllString(repaired, " ")
	return strings.TrimSpace(repaired)
}

func repairJoinedNumberWords(text string) string {
	numberWords := []string{
		"uno", "dos", "tres", "cuatro", "cinco", "seis", "siete", "ocho", "nueve", "diez",
	}
	repaired := text
	for _, left := range numberWords {
		for _, right := range numberWords {
			repaired = strings.ReplaceAll(repaired, left+"y"+right, left+" y "+right)
			repaired = strings.ReplaceAll(repaired, left+right, left+" "+right)
		}
	}
	return repaired
}

func repairJoinedCommonWords(text string) string {
	replacements := []ocrReplacement{
		{old: "tieneuna", new: "tiene una"},
		{old: "tieneun", new: "tiene un"},
		{old: "una duración", new: "una duración"},
		{old: "unaduración", new: "una duración"},
		{old: "unaduracion", new: "una duracion"},
		{old: "duraciónentre", new: "duración entre"},
		{old: "duracionentre", new: "duracion entre"},
		{old: "deentre", new: "de entre"},
		{old: "mesesy", new: "meses y"},
		{old: "ymeses", new: "y meses"},
	}
	repaired := text
	for _, replacement := range replacements {
		repaired = strings.ReplaceAll(repaired, replacement.old, replacement.new)
		repaired = strings.ReplaceAll(repaired, strings.Title(replacement.old), strings.Title(replacement.new))
	}
	return repaired
}

func extractDateOrQuantitySpan(sentence string, pattern *regexp.Regexp) string {
	repaired := applyOCRSpacingRepairs(sentence)
	normalized := NormalizeSearchText(repaired)
	loc := pattern.FindStringIndex(normalized)
	if loc == nil {
		return repaired
	}

	words := strings.Fields(repaired)
	if len(words) == 0 {
		return repaired
	}

	bestStart := 0
	bestEnd := len(words)
	joinedLen := 0
	matchStartWord := -1
	for i := 0; i < len(words); i++ {
		if i > 0 {
			joinedLen++
		}
		word := NormalizeSearchText(words[i])
		joinedLen += len(word)
		wordEnd := joinedLen

		if matchStartWord < 0 && wordEnd > loc[0] {
			matchStartWord = i
		}
		if wordEnd >= loc[1] {
			if matchStartWord < 0 {
				matchStartWord = i
			}
			bestStart = clampLowerInt(matchStartWord-5, 0)
			bestEnd = minInt(len(words), i+6)
			break
		}
	}

	span := strings.Join(words[bestStart:bestEnd], " ")
	if mainVerb := nearestMainVerb(words[bestStart:bestEnd]); mainVerb >= 0 {
		start := clampLowerInt(mainVerb-1, 0)
		end := minInt(len(words[bestStart:bestEnd]), mainVerb+6)
		span = strings.Join(words[bestStart:bestEnd][start:end], " ")
	}
	for _, connector := range []string{"y", "de", "del", "entre"} {
		if strings.HasPrefix(strings.ToLower(span), connector+" ") {
			span = strings.TrimSpace(strings.TrimPrefix(strings.ToLower(span), connector+" "))
			break
		}
	}
	return strings.TrimSpace(span)
}

func nearestMainVerb(words []string) int {
	for i, word := range words {
		normalized := NormalizeSearchText(word)
		switch normalized {
		case "tiene", "dura", "ocurre", "requiere", "alcanza", "es", "tarda", "fue", "sera":
			return i
		}
	}
	return -1
}

func clampLowerInt(value int, minimum int) int {
	if value < minimum {
		return minimum
	}
	return value
}

func excelFieldQueryParts(question string) (string, string) {
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`^que\s+([a-z0-9_ ]+?)\s+tiene\s+([a-z0-9_ .-]+)$`),
		regexp.MustCompile(`^cual\s+es\s+la\s+([a-z0-9_ ]+?)\s+de\s+([a-z0-9_ .-]+)$`),
		regexp.MustCompile(`^cual\s+es\s+el\s+([a-z0-9_ ]+?)\s+de\s+([a-z0-9_ .-]+)$`),
	}

	for _, pattern := range patterns {
		match := pattern.FindStringSubmatch(question)
		if len(match) != 3 {
			continue
		}
		field := strings.TrimSpace(match[1])
		entity := strings.TrimSpace(match[2])
		entity = trimEntityQueryTail(entity)
		if field != "" && entity != "" {
			return field, entity
		}
	}

	return "", ""
}

func trimEntityQueryTail(value string) string {
	for _, suffix := range []string{" del archivo", " del documento", " en el archivo", " en el documento"} {
		value = strings.TrimSpace(strings.TrimSuffix(value, suffix))
	}
	return strings.TrimSpace(value)
}

func parseExcelRowPairs(line string) map[string]string {
	idx := strings.Index(line, ":")
	if idx < 0 || idx >= len(line)-1 {
		return nil
	}

	pairs := strings.Split(line[idx+1:], "|")
	values := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := NormalizeSearchText(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" || value == "" {
			continue
		}
		values[key] = value
	}
	return values
}

func excelRowContainsEntity(values map[string]string, entity string) bool {
	normalizedEntity := NormalizeSearchText(entity)
	if normalizedEntity == "" {
		return false
	}
	for _, value := range values {
		normalizedValue := NormalizeSearchText(value)
		if normalizedValue == normalizedEntity || strings.Contains(normalizedValue, normalizedEntity) {
			return true
		}
	}
	return false
}

func excelMatchField(values map[string]string, field string) (string, bool) {
	normalizedField := NormalizeSearchText(field)
	if normalizedField == "" {
		return "", false
	}

	bestKey := ""
	bestValue := ""
	for key, value := range values {
		if key == normalizedField || strings.Contains(key, normalizedField) || strings.Contains(normalizedField, key) {
			if len(key) > len(bestKey) {
				bestKey = key
				bestValue = value
			}
		}
	}
	if bestKey == "" {
		return "", false
	}
	return bestValue, true
}

func containsAny(text string, terms ...string) bool {
	for _, term := range terms {
		if strings.Contains(text, term) {
			return true
		}
	}
	return false
}
