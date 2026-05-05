package service

import (
	"context"
	"log"
	"math"
	"sort"
	"strings"
	"unicode"

	"auradb-pipeline/internal/repository/postgres"

	"golang.org/x/text/unicode/norm"
)

const (
	defaultSearchTopK     = 3
	minTextScore          = 0.12
	minFinalScore         = 0.22
	minPrecisionScore     = 0.42
	maxResultContentRunes = 900
	duplicateThreshold    = 0.86
	minEntityFilterRunes  = 12
)

type SearchService struct {
	repo             *postgres.SearchRepository
	embeddingService embeddingGenerator
}

type queryIntent struct {
	MainEntity           string
	MainPhrase           string
	MainTerms            []string
	EntityRejectedReason string
	EntityExtractionMode string
}

type topicFocusIntent struct {
	Enabled  bool
	MainTerm string
	Variants []string
}

type embeddingGenerator interface {
	GenerateEmbedding(ctx context.Context, input string) ([]float64, error)
}

func NewSearchService(repo *postgres.SearchRepository) *SearchService {
	return &SearchService{repo: repo}
}

func NewSearchServiceWithEmbedding(repo *postgres.SearchRepository, embeddingService embeddingGenerator) *SearchService {
	return &SearchService{
		repo:             repo,
		embeddingService: embeddingService,
	}
}

func (s *SearchService) Search(ctx context.Context, tenantID string, query string, topK int) ([]postgres.SearchResult, error) {
	return s.SearchWithDocumentIDs(ctx, tenantID, query, topK, nil)
}

func (s *SearchService) GetAllChunksByDocumentID(ctx context.Context, documentID string) ([]postgres.SearchResult, error) {
	results, err := s.repo.GetChunksByDocumentID(ctx, documentID)
	if err != nil {
		return nil, err
	}
	log.Printf("get_chunks_by_document_id document_id=%s chunks_found=%d", documentID, len(results))
	return results, nil
}

func (s *SearchService) SearchWithDocumentID(ctx context.Context, tenantID string, query string, topK int, documentID string) ([]postgres.SearchResult, error) {
	if strings.TrimSpace(documentID) == "" {
		return s.SearchWithDocumentIDs(ctx, tenantID, query, topK, nil)
	}
	return s.SearchWithDocumentIDs(ctx, tenantID, query, topK, []string{strings.TrimSpace(documentID)})
}

func (s *SearchService) SearchWithDocumentIDs(ctx context.Context, tenantID string, query string, topK int, documentIDs []string) ([]postgres.SearchResult, error) {
	topK = normalizeTopK(topK)
	query = NormalizeQuestionForRetrieval(query)
	normalizedQuery := NormalizeSearchText(query)
	queryType := QueryTypeForQuery(query)
	sectionQueryDetected := queryType == "section"
	sectionTerm := SectionTermForQuery(query)
	queryIntent := detectQueryIntent(query)
	topicFocus := detectTopicFocus(query)
	entityFilterApplied := shouldApplyEntityFilter(queryIntent.MainEntity)
	precisionMode := isPrecisionQuery(query) && queryType != "summary"
	selectedDocumentIDs := normalizeDocumentIDs(documentIDs)
	multiDocumentMode := len(selectedDocumentIDs) == 0 || len(selectedDocumentIDs) > 1
	documentIDLog := strings.Join(selectedDocumentIDs, ",")
	if len(selectedDocumentIDs) > 1 && topK > 20 {
		topK = 20
	}
	if queryType == "summary" && topK < 4 {
		topK = 4
	}
	if queryType == "section" && topK < 4 {
		topK = 4
	}
	if queryType == "structure" && topK < 6 {
		topK = 6
	}
	if precisionMode && topK > defaultSearchTopK {
		topK = defaultSearchTopK
	}
	log.Printf("search_request query=%q normalized_question_for_retrieval=%q normalized_query=%q query_type=%q topK=%d document_id=%s selected_document_ids=%q multi_document_mode=%t modo_precision=%t main_terms_detected=%q main_entity_detected=%q entity_filter_applied=%t entity_extraction_mode=%q reason_entity_rejected=%q section_query_detected=%t section_term=%q topic_focused_retrieval=%t main_topic_term=%q", query, query, normalizedQuery, queryType, topK, firstDocumentID(selectedDocumentIDs), documentIDLog, multiDocumentMode, precisionMode, strings.Join(queryIntent.MainTerms, ","), queryIntent.MainEntity, entityFilterApplied, queryIntent.EntityExtractionMode, queryIntent.EntityRejectedReason, sectionQueryDetected, sectionTerm, topicFocus.Enabled, topicFocus.MainTerm)

	if queryType == "summary" {
		return s.searchSummaryDirect(ctx, tenantID, query, topK, selectedDocumentIDs)
	}

	if len(selectedDocumentIDs) > 0 {
		directChunks, err := s.directChunksByDocumentIDs(ctx, tenantID, selectedDocumentIDs)
		if err != nil {
			return nil, err
		}
		log.Printf("search_document_id=%s direct_chunks_found=%d", firstDocumentID(selectedDocumentIDs), len(directChunks))
		log.Printf("multi_document_mode=%t documents_used=%d", multiDocumentMode, countUniqueSearchResultDocuments(directChunks))

		if queryType == "structure" {
			results := orderResultsByDocumentPosition(structureResults(directChunks, topK))
			log.Printf("search_document_id=%s direct_chunks_found=%d text_matches_found=%d returned_chunks=%d", firstDocumentID(selectedDocumentIDs), len(directChunks), len(results), len(results))
			log.Printf("search_mode=text text_matches_found=%d returned_chunks=%d document_id=%s query_type=%s", len(results), len(results), firstDocumentID(selectedDocumentIDs), queryType)
			return results, nil
		}

		textResults, textMatchesFound, boostedChunks := searchDirectTextMatches(query, topK, directChunks)
		if len(textResults) > 0 {
			if topicFocus.Enabled {
				log.Printf("topic_focused_retrieval=true main_topic_term=%q boosted_chunks=%d", topicFocus.MainTerm, boostedChunks)
			}
			log.Printf("search_document_id=%s direct_chunks_found=%d text_matches_found=%d returned_chunks=%d", firstDocumentID(selectedDocumentIDs), len(directChunks), textMatchesFound, len(textResults))
			log.Printf("search_mode=text text_matches_found=%d returned_chunks=%d document_id=%s query_type=%s", textMatchesFound, len(textResults), firstDocumentID(selectedDocumentIDs), queryType)
			return textResults, nil
		}

		fallbackLimit := 3
		if multiDocumentMode {
			fallbackLimit = topK
			if fallbackLimit > 20 {
				fallbackLimit = 20
			}
		}
		fallbackResults := firstDocumentChunks(directChunks, fallbackLimit)
		log.Printf("search_document_id=%s direct_chunks_found=%d text_matches_found=%d returned_chunks=%d", firstDocumentID(selectedDocumentIDs), len(directChunks), textMatchesFound, len(fallbackResults))
		log.Printf("search_mode=fallback text_matches_found=%d returned_chunks=%d document_id=%s query_type=%s", textMatchesFound, len(fallbackResults), firstDocumentID(selectedDocumentIDs), queryType)
		return fallbackResults, nil
	}

	items, err := s.repo.GetChunksWithEmbeddingsByDocumentIDs(ctx, tenantID, selectedDocumentIDs)
	if err != nil {
		return nil, err
	}
	log.Printf("multi_document_mode=%t documents_used=%d", multiDocumentMode, countUniqueSearchResultDocuments(items))

	if len(items) == 0 {
		results, err := s.searchTextFallback(ctx, tenantID, query, topK, selectedDocumentIDs, 0, precisionMode)
		log.Printf("search_mode=fallback text_matches_found=%d returned_chunks=%d document_id=%s query_type=%s", len(results), len(results), firstDocumentID(selectedDocumentIDs), queryType)
		return results, err
	}
	if queryType == "structure" {
		results := structureResults(items, topK)
		log.Printf("search_result mode=structure query=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t candidate_chunks=%d final_chunks=%d", query, normalizedQuery, queryType, firstDocumentID(selectedDocumentIDs), documentIDLog, multiDocumentMode, len(items), len(results))
		return results, nil
	}
	if queryType == "summary" && sectionQueryDetected {
		results, filteredChunksCount := sectionResults(query, items, topK)
		log.Printf("search_result mode=summary_section query=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t candidate_chunks=%d filtered_chunks_count=%d final_chunks=%d section_query_detected=%t section_term=%q", query, normalizedQuery, queryType, firstDocumentID(selectedDocumentIDs), documentIDLog, multiDocumentMode, len(items), filteredChunksCount, len(results), sectionQueryDetected, sectionTerm)
		return results, nil
	}
	if queryType == "section" {
		results, filteredChunksCount := sectionResults(query, items, topK)
		log.Printf("search_result mode=section query=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t candidate_chunks=%d filtered_chunks_count=%d final_chunks=%d section_query_detected=%t section_term=%q", query, normalizedQuery, queryType, firstDocumentID(selectedDocumentIDs), documentIDLog, multiDocumentMode, len(items), filteredChunksCount, len(results), sectionQueryDetected, sectionTerm)
		return results, nil
	}

	queryVector := fakeQueryEmbedding(query)
	if s.embeddingService != nil {
		generatedVector, err := s.embeddingService.GenerateEmbedding(ctx, query)
		if err != nil {
			log.Printf("search_mode=fallback_text query=%q document_id=%s selected_document_ids=%q multi_document_mode=%t modo_precision=%t candidate_chunks=%d reason=embedding_error error=%v", query, firstDocumentID(selectedDocumentIDs), documentIDLog, multiDocumentMode, precisionMode, len(items), err)
			results, fallbackErr := s.searchTextFallback(ctx, tenantID, query, topK, selectedDocumentIDs, len(items), precisionMode)
			log.Printf("search_mode=fallback text_matches_found=%d returned_chunks=%d document_id=%s query_type=%s", len(results), len(results), firstDocumentID(selectedDocumentIDs), queryType)
			return results, fallbackErr
		}
		queryVector = generatedVector
	}

	vectorWeight, textWeight, textBoosted := rankingWeights(query)
	scored := make([]postgres.SearchResult, 0, len(items))
	boostedChunks := 0
	for i := range items {
		textScore := keywordScore(query, items[i].Content)
		if precisionMode && !hasStrongQueryMatch(query, items[i].Content) {
			continue
		}
		if textScore < minTextScore {
			continue
		}

		vectorScore := cosineSimilarity(queryVector, items[i].Embedding)
		finalScore := (vectorScore * vectorWeight) + (textScore * textWeight)
		finalScore += exactEntityBoost(queryIntent, items[i].Content)
		finalScore += specificQuestionBoost(queryIntent, query, items[i].Content)
		if topicBoost := topicFocusedBoost(topicFocus, items[i].Content); topicBoost > 0 {
			finalScore += topicBoost
			boostedChunks++
		}
		if !textBoosted && textScore == 0 {
			finalScore = vectorScore * 0.15
		}
		finalScore *= lengthPenalty(query, items[i].Content, textScore)
		finalScore *= genericSectionPenalty(query, items[i].Content, precisionMode)
		if topicFocus.Enabled {
			finalScore *= topicFocusedPenalty(topicFocus, items[i].Content)
		} else {
			finalScore *= missingMainPhrasePenalty(queryIntent, items[i].Content)
		}
		threshold := minFinalScore
		if precisionMode {
			threshold = minPrecisionScore
		}
		if finalScore < threshold {
			continue
		}

		items[i].Score = finalScore
		items[i].Content = relevantSnippet(query, items[i].Content)
		scored = append(scored, items[i])
	}

	results := finalizeSearchResults(query, scored, topK)
	if topicFocus.Enabled {
		log.Printf("topic_focused_retrieval=true main_topic_term=%q boosted_chunks=%d", topicFocus.MainTerm, boostedChunks)
	}
	exactMatchFound := hasExactIntentMatch(queryIntent, results)
	fallbackLastResortUsed := false
	log.Printf("search_result mode=semantic query=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t modo_precision=%t candidate_chunks=%d final_chunks=%d main_terms_detected=%q main_entity_detected=%q entity_filter_applied=%t entity_extraction_mode=%q reason_entity_rejected=%q exact_entity_match_found=%t fallback_last_resort_used=%t section_query_detected=%t section_term=%q filtered_chunks_count=%d", query, normalizedQuery, queryType, firstDocumentID(selectedDocumentIDs), documentIDLog, multiDocumentMode, precisionMode, len(items), len(results), strings.Join(queryIntent.MainTerms, ","), queryIntent.MainEntity, entityFilterApplied, queryIntent.EntityExtractionMode, queryIntent.EntityRejectedReason, exactMatchFound, fallbackLastResortUsed, sectionQueryDetected, sectionTerm, len(results))
	if len(results) == 0 {
		results = relaxedFallbackResults(query, items)
		log.Printf("search_fallback_relaxed query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t candidate_chunks=%d final_chunks=%d main_terms_detected=%q main_entity_detected=%q entity_filter_applied=%t entity_extraction_mode=%q reason_entity_rejected=%q exact_entity_match_found=%t fallback_last_resort_used=%t section_query_detected=%t section_term=%q filtered_chunks_count=%d", query, queryType, firstDocumentID(selectedDocumentIDs), documentIDLog, multiDocumentMode, len(items), len(results), strings.Join(queryIntent.MainTerms, ","), queryIntent.MainEntity, entityFilterApplied, queryIntent.EntityExtractionMode, queryIntent.EntityRejectedReason, hasExactIntentMatch(queryIntent, results), fallbackLastResortUsed, sectionQueryDetected, sectionTerm, len(results))
	}
	if len(results) == 0 {
		results = lastResortTermResults(query, items, topK)
		fallbackLastResortUsed = len(results) > 0
		log.Printf("search_fallback_last_resort query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t candidate_chunks=%d final_chunks=%d main_terms_detected=%q main_entity_detected=%q entity_filter_applied=%t entity_extraction_mode=%q reason_entity_rejected=%q fallback_last_resort_used=%t section_query_detected=%t section_term=%q filtered_chunks_count=%d", query, queryType, firstDocumentID(selectedDocumentIDs), documentIDLog, multiDocumentMode, len(items), len(results), strings.Join(queryIntent.MainTerms, ","), queryIntent.MainEntity, entityFilterApplied, queryIntent.EntityExtractionMode, queryIntent.EntityRejectedReason, fallbackLastResortUsed, sectionQueryDetected, sectionTerm, len(results))
	}
	if fallbackLastResortUsed {
		log.Printf("search_mode=fallback text_matches_found=%d returned_chunks=%d document_id=%s query_type=%s", len(results), len(results), firstDocumentID(selectedDocumentIDs), queryType)
	} else {
		log.Printf("search_mode=semantic text_matches_found=%d returned_chunks=%d document_id=%s query_type=%s", 0, len(results), firstDocumentID(selectedDocumentIDs), queryType)
	}

	return results, nil
}

func (s *SearchService) searchSummaryDirect(ctx context.Context, tenantID string, query string, topK int, documentIDs []string) ([]postgres.SearchResult, error) {
	items, err := s.repo.GetChunksByDocumentIDs(ctx, tenantID, documentIDs)
	if err != nil {
		return nil, err
	}

	if len(items) == 0 {
		log.Printf(
			"search_result mode=summary_direct query=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t summary_direct_chunk_mode=true summary_chunks_used=%d final_chunks=%d",
			query,
			NormalizeSearchText(query),
			"summary",
			firstDocumentID(documentIDs),
			strings.Join(documentIDs, ","),
			len(documentIDs) == 0 || len(documentIDs) > 1,
			0,
			0,
		)
		return items, nil
	}

	if topK <= 0 {
		topK = 20
	}
	if topK > 20 {
		topK = 20
	}
	if len(items) > topK {
		items = items[:topK]
	}

	log.Printf(
		"search_result mode=summary_direct query=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t summary_direct_chunk_mode=true summary_chunks_used=%d final_chunks=%d",
		query,
		NormalizeSearchText(query),
		"summary",
		firstDocumentID(documentIDs),
		strings.Join(documentIDs, ","),
		len(documentIDs) == 0 || len(documentIDs) > 1,
		len(items),
		len(items),
	)

	return items, nil
}

func (s *SearchService) directChunksByDocumentIDs(ctx context.Context, tenantID string, documentIDs []string) ([]postgres.SearchResult, error) {
	documentIDs = normalizeDocumentIDs(documentIDs)
	if len(documentIDs) == 0 {
		return s.repo.GetChunksByDocumentIDs(ctx, tenantID, nil)
	}
	if len(documentIDs) == 1 {
		return s.repo.GetChunksByDocumentID(ctx, documentIDs[0])
	}
	return s.repo.GetChunksByDocumentIDs(ctx, tenantID, documentIDs)
}

func searchDirectTextMatches(query string, topK int, chunks []postgres.SearchResult) ([]postgres.SearchResult, int, int) {
	topK = normalizeTopK(topK)
	terms := textSearchTerms(query)
	if len(terms) == 0 || len(chunks) == 0 {
		return nil, 0, 0
	}

	matches := filterTextMatches(query, terms, chunks)
	matches = prioritizeTopicMatches(query, matches)
	matchesFound := len(matches)
	boostedChunks := countTopicFocusedChunks(detectTopicFocus(query), matches)
	if len(matches) > topK {
		matches = matches[:topK]
	}
	return matches, matchesFound, boostedChunks
}

func firstDocumentChunks(chunks []postgres.SearchResult, limit int) []postgres.SearchResult {
	if limit <= 0 || len(chunks) == 0 {
		return nil
	}
	ordered := orderResultsByDocumentPosition(chunks)
	if len(ordered) > limit {
		ordered = ordered[:limit]
	}
	for i := range ordered {
		ordered[i].Score = 1.0
	}
	return ordered
}

func countUniqueSearchResultDocuments(chunks []postgres.SearchResult) int {
	seen := make(map[string]bool)
	for _, chunk := range chunks {
		documentID := strings.TrimSpace(chunk.DocumentID)
		if documentID == "" || seen[documentID] {
			continue
		}
		seen[documentID] = true
	}
	return len(seen)
}

func (s *SearchService) searchTextFirst(ctx context.Context, tenantID string, query string, topK int, documentIDs []string) ([]postgres.SearchResult, int, error) {
	topK = normalizeTopK(topK)
	terms := textSearchTerms(query)
	if len(terms) == 0 {
		return nil, 0, nil
	}

	items, err := s.repo.GetChunksByTextByDocumentIDs(ctx, tenantID, documentIDs, strings.Join(terms, " "))
	if err != nil {
		return nil, 0, err
	}

	matches := filterTextMatches(query, terms, items)
	if len(matches) == 0 {
		allItems, err := s.repo.GetChunksByDocumentIDs(ctx, tenantID, documentIDs)
		if err != nil {
			return nil, 0, err
		}
		matches = filterTextMatches(query, terms, allItems)
	}

	sort.SliceStable(matches, func(i, j int) bool {
		if matches[i].DocumentID == matches[j].DocumentID {
			if matches[i].ChunkIndex == matches[j].ChunkIndex {
				return matches[i].ChunkID < matches[j].ChunkID
			}
			return matches[i].ChunkIndex < matches[j].ChunkIndex
		}
		return matches[i].DocumentID < matches[j].DocumentID
	})

	matchesFound := len(matches)
	if len(matches) > topK {
		matches = matches[:topK]
	}
	return matches, matchesFound, nil
}

func textSearchTerms(query string) []string {
	normalized := NormalizeSearchText(NormalizeQuestionForRetrieval(query))
	terms := meaningfulTerms(normalized)
	if len(terms) == 0 {
		return nil
	}

	filtered := make([]string, 0, len(terms))
	seen := make(map[string]bool)
	for _, term := range terms {
		if isSearchIntentWord(term) || seen[term] {
			continue
		}
		seen[term] = true
		filtered = append(filtered, term)
	}
	if len(filtered) > 0 {
		return filtered
	}
	return terms
}

func isSearchIntentWord(term string) bool {
	switch term {
	case "busca", "buscar", "buscame", "encuentra", "encontrar", "dime", "sobre", "documento", "archivo", "texto", "informacion", "consulta", "pregunta", "quiero", "saber", "podrias", "podria", "darme", "dame", "hablame", "explicame":
		return true
	default:
		return false
	}
}

func detectTopicFocus(query string) topicFocusIntent {
	normalized := NormalizeSearchText(NormalizeQuestionForRetrieval(query))
	if normalized == "" {
		return topicFocusIntent{}
	}

	triggerPatterns := []string{
		"sobre ",
		"acerca de ",
		"informacion sobre ",
		"informacion de ",
		"informacion acerca de ",
		"hablame de ",
		"explicame sobre ",
		"detallame sobre ",
	}
	tail := ""
	for _, pattern := range triggerPatterns {
		if idx := strings.LastIndex(normalized, pattern); idx >= 0 {
			tail = strings.TrimSpace(normalized[idx+len(pattern):])
			break
		}
	}

	terms := meaningfulTerms(normalized)
	if tail != "" {
		terms = meaningfulTerms(tail)
	}
	filtered := make([]string, 0, len(terms))
	for _, term := range terms {
		if isSearchIntentWord(term) || isTopicFocusStopWord(term) {
			continue
		}
		filtered = append(filtered, term)
	}
	if len(filtered) == 0 {
		return topicFocusIntent{}
	}

	mainTerm := filtered[len(filtered)-1]
	if !isKnownTopicTerm(mainTerm) && tail == "" {
		return topicFocusIntent{}
	}
	return topicFocusIntent{
		Enabled:  true,
		MainTerm: mainTerm,
		Variants: topicTermVariants(mainTerm),
	}
}

func isTopicFocusStopWord(term string) bool {
	switch term {
	case "este", "esta", "estos", "estas", "tema", "temas", "parte", "partes", "concepto", "conceptos", "contenido", "documentos", "podrias", "podria", "darme", "dame":
		return true
	default:
		return false
	}
}

func isKnownTopicTerm(term string) bool {
	switch term {
	case "arreglo", "arreglos", "array", "arrays", "lista", "listas", "variable", "variables", "funcion", "funciones":
		return true
	default:
		return false
	}
}

func topicTermVariants(term string) []string {
	switch term {
	case "arreglo", "arreglos", "array", "arrays":
		return []string{"arreglo", "arreglos", "array", "arrays"}
	case "lista", "listas":
		return []string{"lista", "listas"}
	case "variable", "variables":
		return []string{"variable", "variables"}
	case "funcion", "funciones":
		return []string{"funcion", "funciones"}
	default:
		variants := []string{term}
		if strings.HasSuffix(term, "s") && len([]rune(term)) > 4 {
			variants = append(variants, strings.TrimSuffix(term, "s"))
		} else {
			variants = append(variants, term+"s")
		}
		return variants
	}
}

func filterTextMatches(query string, terms []string, items []postgres.SearchResult) []postgres.SearchResult {
	if len(terms) == 0 || len(items) == 0 {
		return nil
	}

	matches := make([]postgres.SearchResult, 0, len(items))
	topicFocus := detectTopicFocus(query)
	topicMatchesFound := false
	if topicFocus.Enabled {
		for i := range items {
			if topicFocusedTermCount(topicFocus, items[i].Content) > 0 {
				topicMatchesFound = true
				break
			}
		}
	}
	for i := range items {
		normalizedContent := NormalizeSearchText(items[i].Content)
		if topicMatchesFound && topicFocusedTermCount(topicFocus, items[i].Content) == 0 {
			continue
		}
		if !containsAnyTextSearchTerm(normalizedContent, terms) {
			continue
		}
		item := items[i]
		item.Score = 1.0 + topicFocusedBoost(topicFocus, item.Content)
		item.Content = relevantSnippet(query, item.Content)
		matches = append(matches, item)
	}
	return matches
}

func prioritizeTopicMatches(query string, items []postgres.SearchResult) []postgres.SearchResult {
	topicFocus := detectTopicFocus(query)
	if !topicFocus.Enabled {
		return orderResultsByDocumentPosition(items)
	}
	ordered := append([]postgres.SearchResult(nil), items...)
	sort.SliceStable(ordered, func(i, j int) bool {
		iTopic := topicFocusedTermCount(topicFocus, ordered[i].Content)
		jTopic := topicFocusedTermCount(topicFocus, ordered[j].Content)
		if iTopic != jTopic {
			return iTopic > jTopic
		}
		if ordered[i].Score != ordered[j].Score {
			return ordered[i].Score > ordered[j].Score
		}
		if ordered[i].DocumentID == ordered[j].DocumentID {
			if ordered[i].ChunkIndex == ordered[j].ChunkIndex {
				return ordered[i].ChunkID < ordered[j].ChunkID
			}
			return ordered[i].ChunkIndex < ordered[j].ChunkIndex
		}
		return ordered[i].DocumentID < ordered[j].DocumentID
	})
	return ordered
}

func countTopicFocusedChunks(topicFocus topicFocusIntent, items []postgres.SearchResult) int {
	if !topicFocus.Enabled {
		return 0
	}
	count := 0
	for _, item := range items {
		if topicFocusedTermCount(topicFocus, item.Content) > 0 {
			count++
		}
	}
	return count
}

func orderResultsByDocumentPosition(items []postgres.SearchResult) []postgres.SearchResult {
	ordered := append([]postgres.SearchResult(nil), items...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].DocumentID == ordered[j].DocumentID {
			if ordered[i].ChunkIndex == ordered[j].ChunkIndex {
				return ordered[i].ChunkID < ordered[j].ChunkID
			}
			return ordered[i].ChunkIndex < ordered[j].ChunkIndex
		}
		return ordered[i].DocumentID < ordered[j].DocumentID
	})
	return ordered
}

func containsAnyTextSearchTerm(normalizedContent string, terms []string) bool {
	if normalizedContent == "" {
		return false
	}
	for _, term := range terms {
		if countWholeTerm(normalizedContent, term) > 0 || strings.Contains(normalizedContent, term) {
			return true
		}
	}
	return false
}

func (s *SearchService) searchTextFallback(ctx context.Context, tenantID string, query string, topK int, documentIDs []string, previousCandidates int, precisionMode bool) ([]postgres.SearchResult, error) {
	topK = normalizeTopK(topK)
	query = NormalizeQuestionForRetrieval(query)
	queryType := QueryTypeForQuery(query)
	sectionQueryDetected := queryType == "section"
	sectionTerm := SectionTermForQuery(query)
	queryIntent := detectQueryIntent(query)
	entityFilterApplied := shouldApplyEntityFilter(queryIntent.MainEntity)
	selectedDocumentIDs := normalizeDocumentIDs(documentIDs)
	multiDocumentMode := len(selectedDocumentIDs) == 0 || len(selectedDocumentIDs) > 1
	documentIDLog := strings.Join(selectedDocumentIDs, ",")
	if queryType == "summary" && topK < 4 {
		topK = 4
	}
	if queryType == "section" && topK < 4 {
		topK = 4
	}
	if queryType == "structure" && topK < 6 {
		topK = 6
	}
	if precisionMode && topK > defaultSearchTopK {
		topK = defaultSearchTopK
	}

	items, err := s.repo.GetChunksByTextByDocumentIDs(ctx, tenantID, selectedDocumentIDs, query)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		items, err = s.repo.GetChunksByDocumentIDs(ctx, tenantID, selectedDocumentIDs)
		if err != nil {
			return nil, err
		}
		log.Printf("search_fallback_candidates query=%q normalized_query=%q document_id=%s selected_document_ids=%q multi_document_mode=%t reason=no_exact_text_candidates candidate_chunks=%d", query, NormalizeSearchText(query), firstDocumentID(selectedDocumentIDs), documentIDLog, multiDocumentMode, len(items))
	}
	if queryType == "structure" {
		results := structureResults(items, topK)
		log.Printf("search_result mode=structure query=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t candidate_chunks=%d final_chunks=%d", query, NormalizeSearchText(query), queryType, firstDocumentID(selectedDocumentIDs), documentIDLog, multiDocumentMode, len(items), len(results))
		return results, nil
	}
	if queryType == "summary" && sectionQueryDetected {
		results, filteredChunksCount := sectionResults(query, items, topK)
		log.Printf("search_result mode=summary_section query=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t candidate_chunks=%d filtered_chunks_count=%d final_chunks=%d section_query_detected=%t section_term=%q", query, NormalizeSearchText(query), queryType, firstDocumentID(selectedDocumentIDs), documentIDLog, multiDocumentMode, len(items), filteredChunksCount, len(results), sectionQueryDetected, sectionTerm)
		return results, nil
	}
	if queryType == "section" {
		results, filteredChunksCount := sectionResults(query, items, topK)
		log.Printf("search_result mode=section query=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t candidate_chunks=%d filtered_chunks_count=%d final_chunks=%d section_query_detected=%t section_term=%q", query, NormalizeSearchText(query), queryType, firstDocumentID(selectedDocumentIDs), documentIDLog, multiDocumentMode, len(items), filteredChunksCount, len(results), sectionQueryDetected, sectionTerm)
		return results, nil
	}

	candidateCount := len(items)
	if previousCandidates > candidateCount {
		candidateCount = previousCandidates
	}

	scored := scoreTextFallbackItems(query, items, precisionMode)
	if len(scored) == 0 {
		allItems, err := s.repo.GetChunksByDocumentIDs(ctx, tenantID, selectedDocumentIDs)
		if err != nil {
			return nil, err
		}
		if len(allItems) > len(items) {
			items = allItems
			candidateCount = len(items)
			scored = scoreTextFallbackItems(query, items, precisionMode)
			log.Printf("search_fallback_candidates query=%q normalized_query=%q document_id=%s selected_document_ids=%q multi_document_mode=%t reason=no_ranked_text_candidates candidate_chunks=%d", query, NormalizeSearchText(query), firstDocumentID(selectedDocumentIDs), documentIDLog, multiDocumentMode, len(items))
		}
	}

	results := finalizeSearchResults(query, scored, topK)
	fallbackLastResortUsed := false
	log.Printf("search_result mode=fallback_text query=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t modo_precision=%t candidate_chunks=%d final_chunks=%d main_terms_detected=%q main_entity_detected=%q entity_filter_applied=%t entity_extraction_mode=%q reason_entity_rejected=%q exact_entity_match_found=%t fallback_last_resort_used=%t section_query_detected=%t section_term=%q filtered_chunks_count=%d", query, NormalizeSearchText(query), queryType, firstDocumentID(selectedDocumentIDs), documentIDLog, multiDocumentMode, precisionMode, candidateCount, len(results), strings.Join(queryIntent.MainTerms, ","), queryIntent.MainEntity, entityFilterApplied, queryIntent.EntityExtractionMode, queryIntent.EntityRejectedReason, hasExactIntentMatch(queryIntent, results), fallbackLastResortUsed, sectionQueryDetected, sectionTerm, len(results))
	if len(results) == 0 {
		results = relaxedFallbackResults(query, items)
		log.Printf("search_fallback_relaxed query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t candidate_chunks=%d final_chunks=%d main_terms_detected=%q main_entity_detected=%q entity_filter_applied=%t entity_extraction_mode=%q reason_entity_rejected=%q exact_entity_match_found=%t fallback_last_resort_used=%t section_query_detected=%t section_term=%q filtered_chunks_count=%d", query, queryType, firstDocumentID(selectedDocumentIDs), documentIDLog, multiDocumentMode, len(items), len(results), strings.Join(queryIntent.MainTerms, ","), queryIntent.MainEntity, entityFilterApplied, queryIntent.EntityExtractionMode, queryIntent.EntityRejectedReason, hasExactIntentMatch(queryIntent, results), fallbackLastResortUsed, sectionQueryDetected, sectionTerm, len(results))
	}
	if len(results) == 0 {
		results = lastResortTermResults(query, items, topK)
		fallbackLastResortUsed = len(results) > 0
		log.Printf("search_fallback_last_resort query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t candidate_chunks=%d final_chunks=%d main_terms_detected=%q main_entity_detected=%q entity_filter_applied=%t entity_extraction_mode=%q reason_entity_rejected=%q fallback_last_resort_used=%t section_query_detected=%t section_term=%q filtered_chunks_count=%d", query, queryType, firstDocumentID(selectedDocumentIDs), documentIDLog, multiDocumentMode, len(items), len(results), strings.Join(queryIntent.MainTerms, ","), queryIntent.MainEntity, entityFilterApplied, queryIntent.EntityExtractionMode, queryIntent.EntityRejectedReason, fallbackLastResortUsed, sectionQueryDetected, sectionTerm, len(results))
	}

	return results, nil
}

func lastResortTermResults(query string, items []postgres.SearchResult, topK int) []postgres.SearchResult {
	queryIntent := detectQueryIntent(query)
	topicFocus := detectTopicFocus(query)
	scored := make([]postgres.SearchResult, 0, len(items))
	boostedChunks := 0
	for i := range items {
		score := mainTermOverlapScore(queryIntent, items[i].Content)
		if topicBoost := topicFocusedBoost(topicFocus, items[i].Content); topicBoost > 0 {
			score += topicBoost
			boostedChunks++
		}
		score *= topicFocusedPenalty(topicFocus, items[i].Content)
		if score <= 0 {
			continue
		}
		items[i].Score = score
		items[i].Content = relevantSnippet(query, items[i].Content)
		scored = append(scored, items[i])
	}

	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].Score == scored[j].Score {
			return scored[i].ChunkIndex < scored[j].ChunkIndex
		}
		return scored[i].Score > scored[j].Score
	})

	deduped := make([]postgres.SearchResult, 0, topK)
	for _, item := range scored {
		if isDuplicateResult(item.Content, deduped) {
			continue
		}
		deduped = append(deduped, item)
		if len(deduped) == topK {
			break
		}
	}
	if topicFocus.Enabled {
		log.Printf("topic_focused_retrieval=true main_topic_term=%q boosted_chunks=%d", topicFocus.MainTerm, boostedChunks)
	}
	return deduped
}

func mainTermOverlapScore(intent queryIntent, content string) float64 {
	if len(intent.MainTerms) == 0 {
		return 0
	}

	normalizedContent := NormalizeSearchText(content)
	score := 0.0
	matches := 0
	for _, term := range intent.MainTerms {
		switch {
		case countWholeTerm(normalizedContent, term) > 0:
			score += 1.0
			matches++
		case countApproximateTerm(normalizedContent, term) > 0:
			score += 0.7
			matches++
		case hasPartialTermMatch(normalizedContent, term):
			score += 0.45
			matches++
		}
	}
	if matches == 0 {
		return 0
	}
	score += float64(matches) / float64(len(intent.MainTerms))
	if intent.MainPhrase != "" && containsNormalizedPhrase(content, intent.MainPhrase) {
		score += 0.6
	}
	if shouldApplyEntityFilter(intent.MainEntity) && containsNormalizedPhrase(content, intent.MainEntity) {
		score += 0.8
	}
	return score
}

func scoreTextFallbackItems(query string, items []postgres.SearchResult, precisionMode bool) []postgres.SearchResult {
	scored := make([]postgres.SearchResult, 0, len(items))
	queryIntent := detectQueryIntent(query)
	topicFocus := detectTopicFocus(query)
	boostedChunks := 0
	for i := range items {
		if precisionMode && !hasStrongQueryMatch(query, items[i].Content) {
			continue
		}
		score := keywordScore(query, items[i].Content)
		score += exactEntityBoost(queryIntent, items[i].Content)
		score += specificQuestionBoost(queryIntent, query, items[i].Content)
		if topicBoost := topicFocusedBoost(topicFocus, items[i].Content); topicBoost > 0 {
			score += topicBoost
			boostedChunks++
		}
		threshold := minFinalScore
		if precisionMode {
			threshold = minPrecisionScore
		}
		if score < threshold {
			continue
		}

		items[i].Score = score * lengthPenalty(query, items[i].Content, score)
		items[i].Score *= genericSectionPenalty(query, items[i].Content, precisionMode)
		if topicFocus.Enabled {
			items[i].Score *= topicFocusedPenalty(topicFocus, items[i].Content)
		} else {
			items[i].Score *= missingMainPhrasePenalty(queryIntent, items[i].Content)
		}
		if items[i].Score < threshold {
			continue
		}
		items[i].Content = relevantSnippet(query, items[i].Content)
		scored = append(scored, items[i])
	}
	if topicFocus.Enabled {
		log.Printf("topic_focused_retrieval=true main_topic_term=%q boosted_chunks=%d", topicFocus.MainTerm, boostedChunks)
	}
	return scored
}

func relaxedFallbackResults(query string, items []postgres.SearchResult) []postgres.SearchResult {
	scored := make([]postgres.SearchResult, 0, len(items))
	queryIntent := detectQueryIntent(query)
	topicFocus := detectTopicFocus(query)
	entityFilterApplied := shouldApplyEntityFilter(queryIntent.MainEntity)
	boostedChunks := 0
	for i := range items {
		hasEntityMatch := entityFilterApplied && containsNormalizedPhrase(items[i].Content, queryIntent.MainEntity)
		if !entityFilterApplied && queryIntent.MainPhrase != "" && !containsNormalizedPhrase(items[i].Content, queryIntent.MainPhrase) {
			if !containsMainTerms(queryIntent.MainTerms, items[i].Content) {
				continue
			}
		}
		if !entityFilterApplied && queryIntent.MainPhrase == "" && !containsMainTerms(queryIntent.MainTerms, items[i].Content) {
			continue
		}
		score := relaxedKeywordScore(query, items[i].Content)
		if hasEntityMatch {
			score += 0.45
		}
		score += exactEntityBoost(queryIntent, items[i].Content)
		score += specificQuestionBoost(queryIntent, query, items[i].Content)
		if topicBoost := topicFocusedBoost(topicFocus, items[i].Content); topicBoost > 0 {
			score += topicBoost
			boostedChunks++
		}
		score *= topicFocusedPenalty(topicFocus, items[i].Content)
		if score < 0.08 {
			continue
		}
		items[i].Score = score
		items[i].Content = relevantSnippet(query, items[i].Content)
		scored = append(scored, items[i])
	}

	sort.SliceStable(scored, func(i, j int) bool {
		iScore := scored[i].Score
		jScore := scored[j].Score
		if iScore == jScore {
			return scored[i].ChunkIndex < scored[j].ChunkIndex
		}
		return iScore > jScore
	})

	deduped := make([]postgres.SearchResult, 0, 2)
	for _, item := range scored {
		if isDuplicateResult(item.Content, deduped) {
			continue
		}
		deduped = append(deduped, item)
		if len(deduped) == 2 {
			break
		}
	}

	if topicFocus.Enabled {
		log.Printf("topic_focused_retrieval=true main_topic_term=%q boosted_chunks=%d", topicFocus.MainTerm, boostedChunks)
	}
	return deduped
}

func finalizeSearchResults(query string, items []postgres.SearchResult, topK int) []postgres.SearchResult {
	queryIntent := detectQueryIntent(query)
	topicFocus := detectTopicFocus(query)
	entityFilterApplied := shouldApplyEntityFilter(queryIntent.MainEntity)
	exactFound := false
	for _, item := range items {
		if hasExactIntentMatchInContent(queryIntent, item.Content) {
			exactFound = true
			break
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		if topicFocus.Enabled {
			iTopic := topicFocusedTermCount(topicFocus, items[i].Content)
			jTopic := topicFocusedTermCount(topicFocus, items[j].Content)
			if iTopic != jTopic {
				return iTopic > jTopic
			}
		}
		iIntentExact := hasExactIntentMatchInContent(queryIntent, items[i].Content)
		jIntentExact := hasExactIntentMatchInContent(queryIntent, items[j].Content)
		if iIntentExact != jIntentExact {
			return iIntentExact
		}
		iExact := containsExactPhrase(query, items[i].Content)
		jExact := containsExactPhrase(query, items[j].Content)
		if iExact != jExact {
			return iExact
		}
		iDefinition := isDefinitionLike(query, items[i].Content)
		jDefinition := isDefinitionLike(query, items[j].Content)
		if iDefinition != jDefinition {
			return iDefinition
		}
		iGeneric := isGenericSection(items[i].Content)
		jGeneric := isGenericSection(items[j].Content)
		if iGeneric != jGeneric {
			return !iGeneric
		}
		if items[i].Score == items[j].Score {
			return items[i].ChunkIndex < items[j].ChunkIndex
		}
		return items[i].Score > items[j].Score
	})

	deduped := make([]postgres.SearchResult, 0, len(items))
	for _, item := range items {
		if entityFilterApplied && exactFound && isGenericSection(item.Content) {
			continue
		}
		if isDuplicateResult(item.Content, deduped) {
			continue
		}
		deduped = append(deduped, item)
		if len(deduped) == topK {
			break
		}
	}

	if len(deduped) == 0 && len(items) > 0 {
		limit := topK
		if limit <= 0 || limit > len(items) {
			limit = len(items)
		}
		return append([]postgres.SearchResult(nil), items[:limit]...)
	}

	return deduped
}

func normalizeTopK(topK int) int {
	if topK <= 0 {
		return defaultSearchTopK
	}
	return topK
}

func normalizeDocumentIDs(documentIDs []string) []string {
	if len(documentIDs) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(documentIDs))
	normalized := make([]string, 0, len(documentIDs))
	for _, documentID := range documentIDs {
		documentID = strings.TrimSpace(documentID)
		if documentID == "" || seen[documentID] {
			continue
		}
		seen[documentID] = true
		normalized = append(normalized, documentID)
	}
	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

func firstDocumentID(documentIDs []string) string {
	if len(documentIDs) == 0 {
		return ""
	}
	return documentIDs[0]
}

func QueryTypeForQuery(query string) string {
	if isStructureQuery(query) {
		return "structure"
	}
	if isSummaryQuery(query) {
		return "summary"
	}
	if SectionQueryDetected(query) {
		return "section"
	}
	return "question"
}

func SectionQueryDetected(query string) bool {
	normalizedQuery := NormalizeSearchText(query)
	if strings.HasPrefix(normalizedQuery, "sobre ") {
		return strings.TrimSpace(normalizedQuery[len("sobre "):]) != ""
	}

	sectionPatterns := []string{
		"seccion de ",
		"parte de ",
		"apartado de ",
		"tema de ",
		"hoja ",
	}
	for _, pattern := range sectionPatterns {
		if strings.Contains(normalizedQuery, pattern) {
			return true
		}
	}
	return false
}

func SectionTermForQuery(query string) string {
	normalizedQuery := NormalizeSearchText(query)
	patterns := []string{
		"seccion de ",
		"parte de ",
		"apartado de ",
		"tema de ",
		"hoja ",
	}
	for _, pattern := range patterns {
		if idx := strings.Index(normalizedQuery, pattern); idx >= 0 {
			return cleanSectionTerm(normalizedQuery[idx+len(pattern):])
		}
	}
	if strings.HasPrefix(normalizedQuery, "sobre ") {
		return cleanSectionTerm(normalizedQuery[len("sobre "):])
	}
	return ""
}

func isStructureQuery(query string) bool {
	normalizedQuery := NormalizeSearchText(query)
	structurePatterns := []string{
		"secciones del documento",
		"estructura del documento",
		"estructura de la hoja",
		"indice",
		"indice del documento",
		"temas del documento",
		"que contiene el documento",
		"que contiene la hoja",
		"que contiene hoja",
		"que columnas aparecen",
		"que columnas tiene",
		"que columnas tiene este archivo",
		"que columnas tiene el archivo",
		"columnas aparecen",
		"columnas tiene",
	}
	for _, pattern := range structurePatterns {
		if strings.Contains(normalizedQuery, pattern) {
			return true
		}
	}
	return false
}

func isSummaryQuery(query string) bool {
	normalizedQuery := NormalizeSearchText(query)
	summaryPatterns := []string{
		"resume la parte de",
		"resumen de",
		"resumen del documento",
		"resumir documento",
		"resumir este documento",
		"resume",
		"que dice sobre",
		"explicame este documento",
		"explicame el documento",
		"explica este documento",
		"explica el documento",
		"explica la seccion de",
		"explica la seccion",
		"explica el apartado",
		"analiza este documento",
		"analiza el documento",
		"puntos clave de",
		"puntos clave",
		"sintesis de",
		"resumen del tema",
		"resume la hoja",
		"resume hoja",
		"resumen de la hoja",
		"resumen hoja",
	}
	for _, pattern := range summaryPatterns {
		if strings.Contains(normalizedQuery, pattern) {
			return true
		}
	}
	return false
}

func cleanSectionTerm(term string) string {
	term = strings.TrimSpace(term)
	if term == "" {
		return ""
	}

	noisePhrases := []string{
		"del documento",
		"del archivo",
		"en el documento",
		"en el archivo",
	}
	for _, phrase := range noisePhrases {
		term = strings.ReplaceAll(term, phrase, "")
	}

	for _, prefix := range []string{"la ", "el ", "los ", "las ", "un ", "una "} {
		if strings.HasPrefix(term, prefix) {
			term = strings.TrimSpace(strings.TrimPrefix(term, prefix))
			break
		}
	}

	terms := meaningfulTerms(term)
	if len(terms) == 0 {
		return ""
	}
	return strings.TrimSpace(strings.Join(terms, " "))
}

func rankingWeights(query string) (float64, float64, bool) {
	if hasQuestionTextBoost(query) {
		return 0.3, 0.7, true
	}
	return 0.4, 0.6, false
}

func structureResults(items []postgres.SearchResult, topK int) []postgres.SearchResult {
	if topK <= 0 {
		topK = 6
	}

	scored := make([]postgres.SearchResult, 0, len(items))
	for i := range items {
		cleaned := cleanStructureText(items[i].Content)
		if cleaned == "" {
			continue
		}
		items[i].Content = cleaned
		score := structureChunkScore(items[i])
		if score <= 0 {
			continue
		}
		items[i].Score = score
		scored = append(scored, items[i])
	}

	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].Score == scored[j].Score {
			return scored[i].ChunkIndex < scored[j].ChunkIndex
		}
		return scored[i].Score > scored[j].Score
	})

	deduped := make([]postgres.SearchResult, 0, topK)
	for _, item := range scored {
		if isDuplicateResult(item.Content, deduped) {
			continue
		}
		deduped = append(deduped, item)
		if len(deduped) == topK {
			break
		}
	}

	if len(deduped) > 0 {
		return deduped
	}

	if len(items) <= topK {
		return items
	}
	return items[:topK]
}

func sectionResults(query string, items []postgres.SearchResult, topK int) ([]postgres.SearchResult, int) {
	if topK <= 0 {
		topK = 4
	}

	sectionTerm := SectionTermForQuery(query)
	if sectionTerm == "" {
		results := lastResortTermResults(query, items, topK)
		return results, len(results)
	}

	anchorIndexes := make([]int, 0)
	anchorScores := make(map[int]float64)
	for i := range items {
		score := sectionContentScore(sectionTerm, items[i].Content)
		if score <= 0 {
			continue
		}
		anchorIndexes = append(anchorIndexes, items[i].ChunkIndex)
		anchorScores[items[i].ChunkIndex] = score
	}

	if len(anchorIndexes) == 0 {
		scored := make([]postgres.SearchResult, 0, len(items))
		for i := range items {
			score := mainTermOverlapScore(detectQueryIntent(sectionTerm), items[i].Content)
			if score <= 0 {
				continue
			}
			items[i].Score = score
			items[i].Content = relevantSnippet(sectionTerm, items[i].Content)
			scored = append(scored, items[i])
		}
		sort.SliceStable(scored, func(i, j int) bool {
			if scored[i].Score == scored[j].Score {
				return scored[i].ChunkIndex < scored[j].ChunkIndex
			}
			return scored[i].Score > scored[j].Score
		})
		if len(scored) > topK {
			scored = scored[:topK]
		}
		return scored, len(scored)
	}

	scored := make([]postgres.SearchResult, 0, len(items))
	for i := range items {
		directScore := sectionContentScore(sectionTerm, items[i].Content)
		distance := nearestChunkDistance(items[i].ChunkIndex, anchorIndexes)
		proximityScore := sectionProximityScore(distance)
		if directScore <= 0 && proximityScore <= 0 {
			continue
		}

		score := directScore*3.0 + proximityScore
		if existingScore, ok := anchorScores[items[i].ChunkIndex]; ok && existingScore > directScore {
			score = existingScore*3.0 + proximityScore
		}

		items[i].Score = score
		items[i].Content = relevantSnippet(sectionTerm, items[i].Content)
		scored = append(scored, items[i])
	}

	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].Score == scored[j].Score {
			return scored[i].ChunkIndex < scored[j].ChunkIndex
		}
		return scored[i].Score > scored[j].Score
	})

	filteredCount := len(scored)
	deduped := make([]postgres.SearchResult, 0, topK)
	for _, item := range scored {
		if isDuplicateResult(item.Content, deduped) {
			continue
		}
		deduped = append(deduped, item)
		if len(deduped) == topK {
			break
		}
	}

	return deduped, filteredCount
}

func sectionContentScore(sectionTerm string, content string) float64 {
	normalizedContent := NormalizeSearchText(content)
	normalizedTerm := NormalizeSearchText(sectionTerm)
	if normalizedContent == "" || normalizedTerm == "" {
		return 0
	}

	score := 0.0
	if strings.Contains(normalizedContent, normalizedTerm) {
		score += 1.8
	}

	termWords := meaningfulTerms(normalizedTerm)
	if len(termWords) == 0 {
		return score
	}

	matches := 0
	for _, term := range termWords {
		switch {
		case countWholeTerm(normalizedContent, term) > 0:
			score += 0.8
			matches++
		case countApproximateTerm(normalizedContent, term) > 0:
			score += 0.5
			matches++
		case hasPartialTermMatch(normalizedContent, term):
			score += 0.3
			matches++
		}
	}

	if matches == 0 {
		return 0
	}
	score += float64(matches) / float64(len(termWords))
	return score
}

func nearestChunkDistance(chunkIndex int, anchorIndexes []int) int {
	if len(anchorIndexes) == 0 {
		return -1
	}
	best := -1
	for _, anchorIndex := range anchorIndexes {
		distance := absInt(chunkIndex - anchorIndex)
		if best < 0 || distance < best {
			best = distance
		}
	}
	return best
}

func sectionProximityScore(distance int) float64 {
	switch distance {
	case 0:
		return 1.2
	case 1:
		return 0.8
	case 2:
		return 0.35
	default:
		return 0
	}
}

func structureChunkScore(item postgres.SearchResult) float64 {
	content := cleanStructureText(item.Content)
	if content == "" {
		return 0
	}

	score := 0.3
	lines := strings.Split(content, "\n")
	firstLine := strings.TrimSpace(lines[0])
	if isLikelyHeading(firstLine) {
		score += 2.4
	}
	if len(lines) > 1 && isLikelyHeading(strings.TrimSpace(lines[1])) {
		score += 1.1
	}
	if strings.Contains(content, ":") {
		score += 0.35
	}
	if strings.Contains(content, "Hoja:") || strings.Contains(content, "Columnas:") {
		score += 1.25
	}
	if strings.Contains(content, "Total de filas de datos:") {
		score += 0.75
	}
	if strings.Contains(content, "\n- ") || strings.Contains(content, "\n•") {
		score += 0.4
	}
	if item.ChunkIndex < 8 {
		score += 0.6
	}

	runeLen := len([]rune(content))
	switch {
	case runeLen >= 80 && runeLen <= 550:
		score += 0.8
	case runeLen < 80:
		score += 0.2
	case runeLen > 900:
		score -= 0.4
	}

	titleWords := 0
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if isLikelyHeading(line) {
			titleWords++
		}
	}
	score += float64(titleWords) * 0.25
	return score
}

func cleanStructureText(text string) string {
	text = cleanExtractedText(text)
	if text == "" {
		return text
	}

	lines := strings.Split(text, "\n")
	filtered := make([]string, 0, len(lines))
	seen := make(map[string]bool)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if isAcademicNoiseLine(line) {
			continue
		}

		normalized := NormalizeSearchText(line)
		if normalized == "" {
			continue
		}
		if containsStructureNoise(normalized) {
			continue
		}

		if isLikelyHeading(line) {
			line = normalizeHeadingCapitalization(line)
		}

		key := NormalizeSearchText(line)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		filtered = append(filtered, line)
	}

	if len(filtered) == 0 {
		return ""
	}

	return strings.Join(filtered, "\n")
}

func containsStructureNoise(normalized string) bool {
	for _, term := range []string{
		"fuentes",
		"referencias",
		"bibliografia",
		"bibliografia consultada",
		"isbn",
		"editorial",
		"press",
		"springer",
		"pearson",
		"mcgraw",
		"routledge",
		"wiley",
		"autor",
		"autores",
	} {
		if strings.Contains(normalized, term) {
			return true
		}
	}
	return false
}

func hasQuestionTextBoost(query string) bool {
	query = normalizeText(query)
	boostTerms := []string{"fecha", "dia", "cuando", "audiencia", "quien", "dirigido", "observaciones", "fila", "filas", "registro", "registros", "columna", "columnas", "hoja", "tabla"}
	for _, term := range boostTerms {
		if strings.Contains(query, term) {
			return true
		}
	}
	return false
}

func fakeQueryEmbedding(query string) []float64 {
	vec := make([]float64, 10)
	base := float64(len(strings.TrimSpace(query)))
	for i := range vec {
		vec[i] = math.Mod(base+float64(i)*0.37, 1.0)
	}
	return vec
}

func cosineSimilarity(a, b []float64) float64 {
	if len(a) == 0 || len(b) == 0 || len(a) != len(b) {
		return 0
	}

	var dot, normA, normB float64
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

func keywordScore(query, content string) float64 {
	normalizedQuery := NormalizeSearchText(query)
	normalizedContent := NormalizeSearchText(content)
	if normalizedQuery == "" || normalizedContent == "" {
		return 0
	}

	terms := meaningfulTerms(normalizedQuery)
	if len(terms) == 0 {
		return 0
	}

	score := 0.0
	if strings.Contains(normalizedContent, normalizedQuery) {
		score += 0.85
	}

	matches := 0
	totalOccurrences := 0
	mainTermOccurrences := 0
	mainTerm := terms[0]

	for _, term := range terms {
		occurrences := countWholeTerm(normalizedContent, term)
		if occurrences == 0 {
			if strings.Contains(normalizedContent, term) {
				score += 0.08
			} else if term == mainTerm {
				occurrences = countApproximateTerm(normalizedContent, term)
				if occurrences > 0 {
					score += 0.22
				}
			}
			if occurrences == 0 {
				continue
			}
		}
		matches++
		totalOccurrences += occurrences
		if term == mainTerm {
			mainTermOccurrences = occurrences
		}
	}

	coverage := float64(matches) / float64(len(terms))
	score += coverage * 0.50
	score += math.Min(float64(totalOccurrences)*0.04, 0.20)
	score += math.Min(float64(mainTermOccurrences)*0.06, 0.18)
	if isDefinitionLike(query, content) {
		score += 0.35
	}

	if matches >= 2 {
		score += 0.12
	}
	if matches == 0 {
		return 0
	}

	return score
}

func relaxedKeywordScore(query, content string) float64 {
	normalizedQuery := NormalizeSearchText(query)
	normalizedContent := NormalizeSearchText(content)
	if normalizedQuery == "" || normalizedContent == "" {
		return 0
	}

	terms := meaningfulTerms(normalizedQuery)
	if len(terms) == 0 {
		return 0
	}

	score := 0.0
	if strings.Contains(normalizedContent, normalizedQuery) {
		score += 0.55
	}

	matches := 0
	mainTerm := terms[0]
	for _, term := range terms {
		switch {
		case countWholeTerm(normalizedContent, term) > 0:
			score += 0.34
			matches++
		case strings.Contains(normalizedContent, term):
			score += 0.18
			matches++
		case term == mainTerm && countApproximateTerm(normalizedContent, term) > 0:
			score += 0.26
			matches++
		case term == mainTerm && hasPartialTermMatch(normalizedContent, term):
			score += 0.12
			matches++
		}
	}

	if matches == 0 {
		return 0
	}

	score += (float64(matches) / float64(len(terms))) * 0.18
	if isDefinitionLike(query, content) {
		score += 0.12
	}
	return score
}

func topicFocusedBoost(topicFocus topicFocusIntent, content string) float64 {
	if !topicFocus.Enabled {
		return 0
	}
	count := topicFocusedTermCount(topicFocus, content)
	if count == 0 {
		return 0
	}
	boost := 1.2 + math.Min(float64(count)*0.16, 0.8)
	if isTopicDefinitionLike(topicFocus, content) {
		boost += 0.45
	}
	return boost
}

func topicFocusedPenalty(topicFocus topicFocusIntent, content string) float64 {
	if !topicFocus.Enabled {
		return 1
	}
	if topicFocusedTermCount(topicFocus, content) > 0 {
		return 1
	}
	if isGenericProgrammingContent(content) {
		return 0.18
	}
	return 0.42
}

func topicFocusedTermCount(topicFocus topicFocusIntent, content string) int {
	if !topicFocus.Enabled {
		return 0
	}
	normalizedContent := NormalizeSearchText(content)
	count := 0
	seen := make(map[string]bool, len(topicFocus.Variants))
	for _, variant := range topicFocus.Variants {
		variant = NormalizeSearchText(variant)
		if variant == "" || seen[variant] {
			continue
		}
		seen[variant] = true
		count += countWholeTerm(normalizedContent, variant)
	}
	return count
}

func isTopicDefinitionLike(topicFocus topicFocusIntent, content string) bool {
	if !topicFocus.Enabled {
		return false
	}
	normalizedContent := NormalizeSearchText(content)
	for _, variant := range topicFocus.Variants {
		variant = NormalizeSearchText(variant)
		if variant == "" {
			continue
		}
		for _, pattern := range []string{
			variant + " es ",
			variant + " son ",
			variant + " se define ",
			variant + " consiste ",
			variant + " permite ",
			"un " + variant + " ",
			"una " + variant + " ",
			"los " + variant + " ",
			"las " + variant + " ",
		} {
			if strings.Contains(normalizedContent, pattern) {
				return true
			}
		}
	}
	return false
}

func isGenericProgrammingContent(content string) bool {
	normalizedContent := NormalizeSearchText(content)
	if normalizedContent == "" {
		return false
	}
	genericHits := 0
	for _, phrase := range []string{
		"logica de programacion",
		"fundamentos de programacion",
		"programacion general",
		"conceptos basicos",
		"estructuras de control",
		"algoritmos",
		"lenguajes de programacion",
		"introduccion",
	} {
		if strings.Contains(normalizedContent, phrase) {
			genericHits++
		}
	}
	return genericHits >= 1
}

func detectQueryIntent(query string) queryIntent {
	normalizedQuery := NormalizeSearchText(query)
	terms := meaningfulTerms(normalizedQuery)
	intent := queryIntent{
		MainTerms: make([]string, 0, len(terms)),
	}

	for _, term := range terms {
		if len([]rune(term)) > 4 {
			intent.MainTerms = append(intent.MainTerms, term)
		}
	}
	if len(intent.MainTerms) == 0 {
		intent.MainTerms = terms
	}

	intent.MainEntity, intent.EntityRejectedReason, intent.EntityExtractionMode = detectMainEntity(normalizedQuery)
	intent.MainPhrase = longestSignificantPhrase(normalizedQuery)
	return intent
}

func MainEntityForQuery(query string) string {
	return detectQueryIntent(query).MainEntity
}

func MainEntityRejectedReasonForQuery(query string) string {
	return detectQueryIntent(query).EntityRejectedReason
}

func EntityExtractionModeForQuery(query string) string {
	return detectQueryIntent(query).EntityExtractionMode
}

func ShouldApplyMainEntityFilter(query string) bool {
	return shouldApplyEntityFilter(detectQueryIntent(query).MainEntity)
}

func ContentContainsMainEntity(content string, entity string) bool {
	return !shouldApplyEntityFilter(entity) || containsNormalizedPhrase(content, entity)
}

func detectMainEntity(normalizedQuery string) (string, string, string) {
	rawTerms := strings.Fields(normalizedQuery)
	if len(rawTerms) == 0 {
		return "", "empty_query", "rejected"
	}

	if entity, mode := extractSpecificTrailingEntity(rawTerms); entity != "" {
		return entity, "", mode
	}

	for size := minInt(4, len(rawTerms)); size >= 2; size-- {
		for start := 0; start+size <= len(rawTerms); start++ {
			candidateTerms := rawTerms[start : start+size]
			candidate := strings.Join(candidateTerms, " ")
			entity, reason := validateMainEntityCandidate(candidateTerms, candidate)
			if entity != "" {
				return entity, "", "candidate_window"
			}
			if start == 0 && size == 2 && reason != "" {
				return "", reason, "rejected"
			}
		}
	}

	return "", "no_strong_entity_candidate", "rejected"
}

func longestSignificantPhrase(normalizedQuery string) string {
	terms := meaningfulTerms(normalizedQuery)
	if len(terms) < 2 {
		return ""
	}

	best := ""
	for size := len(terms); size >= 2; size-- {
		for start := 0; start+size <= len(terms); start++ {
			phraseTerms := terms[start : start+size]
			if !hasLongTerm(phraseTerms) {
				continue
			}
			phrase := strings.Join(phraseTerms, " ")
			if len([]rune(phrase)) > len([]rune(best)) {
				best = phrase
			}
		}
		if best != "" {
			return best
		}
	}
	return ""
}

func hasLongTerm(terms []string) bool {
	for _, term := range terms {
		if len([]rune(term)) > 4 {
			return true
		}
	}
	return false
}

func exactEntityBoost(intent queryIntent, content string) float64 {
	boost := 0.0
	normalizedContent := NormalizeSearchText(content)
	if shouldApplyEntityFilter(intent.MainEntity) && strings.Contains(normalizedContent, intent.MainEntity) {
		boost += 1.6
	}
	if intent.MainPhrase != "" && containsNormalizedPhrase(content, intent.MainPhrase) {
		boost += 1.0
	}
	for _, term := range intent.MainTerms {
		if countWholeTerm(normalizedContent, term) > 0 {
			boost += 0.18
			continue
		}
		if countApproximateTerm(normalizedContent, term) > 0 {
			boost += 0.06
		}
	}
	return boost
}

func missingMainPhrasePenalty(intent queryIntent, content string) float64 {
	if shouldApplyEntityFilter(intent.MainEntity) {
		if containsNormalizedPhrase(content, intent.MainEntity) {
			return 1
		}
		return 0.72
	}
	if intent.MainPhrase == "" {
		return 1
	}
	if containsNormalizedPhrase(content, intent.MainPhrase) {
		return 1
	}
	return 0.25
}

func hasExactIntentMatch(intent queryIntent, items []postgres.SearchResult) bool {
	for _, item := range items {
		if hasExactIntentMatchInContent(intent, item.Content) {
			return true
		}
	}
	return false
}

func hasExactIntentMatchInContent(intent queryIntent, content string) bool {
	normalizedContent := NormalizeSearchText(content)
	if shouldApplyEntityFilter(intent.MainEntity) {
		return strings.Contains(normalizedContent, intent.MainEntity)
	}
	if intent.MainPhrase != "" && containsNormalizedPhrase(content, intent.MainPhrase) {
		return true
	}
	for _, term := range intent.MainTerms {
		if countWholeTerm(normalizedContent, term) > 0 {
			return true
		}
	}
	return false
}

func specificQuestionBoost(intent queryIntent, query string, content string) float64 {
	if !isSpecificFactQuery(query) && !isTabularQuery(query) {
		return 0
	}
	normalizedContent := NormalizeSearchText(content)
	boost := 0.0
	if shouldApplyEntityFilter(intent.MainEntity) && strings.Contains(normalizedContent, intent.MainEntity) {
		boost += 0.35
	}
	if containsYear(normalizedContent) {
		boost += 0.25
	}
	for _, term := range []string{"anuncio", "anuncio", "descubrio", "lidero", "detecto", "confirmo", "publico", "presento"} {
		if strings.Contains(normalizedContent, term) {
			boost += 0.18
			break
		}
	}
	if isTabularQuery(query) {
		for _, term := range []string{"hoja", "columnas", "columna", "fila", "filas", "registro", "registros", "tabla", "encabezados"} {
			if strings.Contains(normalizedContent, term) {
				boost += 0.16
			}
		}
	}
	return boost
}

func isSpecificFactQuery(query string) bool {
	normalizedQuery := NormalizeSearchText(query)
	for _, term := range []string{"quien", "cuando", "ano", "fecha"} {
		if strings.Contains(normalizedQuery, term) {
			return true
		}
	}
	return false
}

func isTabularQuery(query string) bool {
	normalizedQuery := NormalizeSearchText(query)
	for _, term := range []string{"hoja", "tabla", "fila", "filas", "columna", "columnas", "registro", "registros", "encabezado", "encabezados"} {
		if strings.Contains(normalizedQuery, term) {
			return true
		}
	}
	return false
}

func containsYear(text string) bool {
	for _, word := range strings.Fields(text) {
		if len(word) != 4 {
			continue
		}
		year := true
		for _, r := range word {
			if r < '0' || r > '9' {
				year = false
				break
			}
		}
		if year {
			return true
		}
	}
	return false
}

func isEntityQueryStopWord(term string) bool {
	switch term {
	case "a", "al", "el", "la", "los", "las", "de", "del", "que", "se", "en", "un", "una", "unos", "unas", "y", "o", "para", "por", "con", "su", "sus", "es",
		"quien", "quienes", "cuando", "ano", "fecha", "cual", "cuales", "como", "donde", "dime", "explica", "informacion", "sobre", "acerca",
		"fue", "son", "esta", "este", "esa", "ese", "lo", "le", "me", "respondeme", "respuesta",
		"anuncio", "anunciado", "anunciada", "descubrio", "descubierto", "descubierta", "lidero", "liderado", "detecto", "detectado", "confirmo", "confirmado", "publico", "publicado", "presento", "presentado",
		"descubrimiento", "hallazgo", "deteccion", "publicacion":
		return true
	default:
		return false
	}
}

func isGenericEntityTerm(term string) bool {
	switch term {
	case "contador", "acumulador", "variable", "variables", "dato", "datos", "valor", "valores", "campo", "campos", "sistema", "proceso", "procesos", "documento", "documentos", "codigo", "funcion", "funciones", "metodo", "metodos", "clase", "clases", "objeto", "objetos", "lista", "listas", "tipo", "tipos", "modulo", "modulos":
		return true
	default:
		return false
	}
}

func isGenericEntityPhrase(entity string) bool {
	if entity == "" {
		return true
	}
	genericPhrases := []string{
		"diferencia entre",
		"que es",
		"cual es",
		"como funciona",
		"cuando se",
		"quien es",
		"tipo de",
		"forma de",
		"parte de",
		"informacion sobre",
		"acerca de",
		"palabras clave",
		"preguntas de repaso",
	}
	for _, phrase := range genericPhrases {
		if entity == phrase {
			return true
		}
	}
	return false
}

func validateMainEntityCandidate(candidateTerms []string, candidate string) (string, string) {
	if len(candidateTerms) < 2 {
		return "", "entity_too_short"
	}
	candidate = strings.TrimSpace(candidate)
	if isGenericEntityPhrase(candidate) {
		return "", "generic_entity_phrase"
	}
	if isInterrogativeOrConnector(candidateTerms[0]) || hasWeakEntityTrailingBoundary(candidateTerms) {
		return "", "generic_boundary_term"
	}
	if containsWeakEntityTerms(candidateTerms) && !hasClearlyProperRealEntity(candidateTerms) {
		return "", "weak_entity_phrase"
	}
	relevantCount := 0
	hasStrongNoun := false
	for _, term := range candidateTerms {
		if !isGenericEntityTerm(term) && !isEntityQueryStopWord(term) {
			relevantCount++
		}
		if isStrongEntityTerm(term) {
			hasStrongNoun = true
		}
	}
	if relevantCount < 2 {
		return "", "insufficient_relevant_terms"
	}
	if containsWeakStructuralTerms(candidateTerms) && !hasProperLikeEntity(candidateTerms) {
		return "", "structural_phrase_without_proper_name"
	}
	if !hasStrongNoun && !hasProperLikeEntity(candidateTerms) {
		return "", "missing_strong_noun"
	}
	return candidate, ""
}

func extractSpecificTrailingEntity(rawTerms []string) (string, string) {
	triggerIndex := -1
	for i := 0; i < len(rawTerms)-1; i++ {
		if !isEntityTriggerTerm(rawTerms[i]) {
			continue
		}
		if rawTerms[i+1] == "de" {
			triggerIndex = i + 2
			continue
		}
		if rawTerms[i+1] == "del" {
			triggerIndex = i + 2
		}
	}
	if triggerIndex < 0 || triggerIndex >= len(rawTerms) {
		return "", ""
	}

	candidateTerms := trimEntityBoundaryTerms(rawTerms[triggerIndex:])
	candidateTerms = trimNarrativeResidue(candidateTerms)
	if len(candidateTerms) < 2 {
		return "", ""
	}

	if entity, _ := validateSpecificEntityCandidate(candidateTerms); entity != "" {
		return entity, "trigger_tail"
	}

	for start := 0; start < len(candidateTerms)-1; start++ {
		if entity, _ := validateSpecificEntityCandidate(candidateTerms[start:]); entity != "" {
			return entity, "trigger_tail"
		}
	}

	return "", ""
}

func validateSpecificEntityCandidate(candidateTerms []string) (string, string) {
	if len(candidateTerms) < 2 {
		return "", "entity_too_short"
	}
	candidateTerms = trimEntityBoundaryTerms(candidateTerms)
	if len(candidateTerms) < 2 {
		return "", "entity_too_short"
	}
	if hasWeakEntityTrailingBoundary(candidateTerms) {
		return "", "generic_boundary_term"
	}
	if containsWeakEntityTerms(candidateTerms) && !hasClearlyProperRealEntity(candidateTerms) {
		return "", "weak_entity_phrase"
	}

	relevantCount := 0
	hasStrongNoun := false
	hasSpecificSuffix := false
	for _, term := range candidateTerms {
		if !isEntityQueryStopWord(term) && !isInterrogativeOrConnector(term) {
			relevantCount++
		}
		if isStrongEntityTerm(term) {
			hasStrongNoun = true
		}
		if isSpecificEntitySuffix(term) {
			hasSpecificSuffix = true
		}
	}
	if relevantCount < 2 {
		return "", "insufficient_relevant_terms"
	}
	if hasStrongNoun || hasSpecificSuffix || hasProperLikeEntity(candidateTerms) {
		return strings.Join(candidateTerms, " "), ""
	}
	return "", "missing_strong_noun"
}

func isInterrogativeOrConnector(term string) bool {
	switch term {
	case "diferencia", "entre", "cual", "que", "como", "cuando", "quien", "donde", "sobre", "acerca", "tipo", "forma", "parte", "de", "del", "la", "el", "los", "las", "un", "una":
		return true
	default:
		return false
	}
}

func isEntityTriggerTerm(term string) bool {
	switch term {
	case "descubrimiento", "anuncio", "hallazgo", "deteccion", "descubrio", "lidero", "publico", "presento":
		return true
	default:
		return false
	}
}

func trimEntityBoundaryTerms(terms []string) []string {
	for len(terms) > 0 && isEntityLeadingClassifier(terms[0]) {
		terms = terms[1:]
	}
	for len(terms) > 0 && isInterrogativeOrConnector(terms[0]) {
		terms = terms[1:]
	}
	for len(terms) > 0 && isEntityTrailingStopWord(terms[len(terms)-1]) {
		terms = terms[:len(terms)-1]
	}
	return terms
}

func trimNarrativeResidue(terms []string) []string {
	for len(terms) > 0 && isNarrativeEntityTerm(terms[0]) {
		terms = terms[1:]
	}
	for len(terms) > 0 && isNarrativeEntityTerm(terms[len(terms)-1]) {
		terms = terms[:len(terms)-1]
	}
	if len(terms) >= 3 && terms[0] == "proxima" && terms[1] == "centauri" {
		return terms
	}
	for start := 0; start < len(terms); start++ {
		window := terms[start:]
		if len(window) >= 2 && looksSpecificNamedEntity(window) {
			return window
		}
	}
	return terms
}

func isNarrativeEntityTerm(term string) bool {
	switch term {
	case "descubrimiento", "anuncio", "hallazgo", "deteccion", "detección", "lidero", "lideró", "publico", "publicó", "presento", "presentó":
		return true
	default:
		return false
	}
}

func looksSpecificNamedEntity(terms []string) bool {
	if len(terms) < 2 {
		return false
	}
	specificCount := 0
	for _, term := range terms {
		if isNarrativeEntityTerm(term) || isInterrogativeOrConnector(term) || isEntityQueryStopWord(term) {
			continue
		}
		if isStrongEntityTerm(term) || isSpecificEntitySuffix(term) || len([]rune(term)) >= 6 {
			specificCount++
		}
	}
	return specificCount >= 2 || (specificCount >= 1 && len(terms) >= 3)
}

func isEntityLeadingClassifier(term string) bool {
	switch term {
	case "planeta", "estrella", "sistema", "objeto":
		return true
	default:
		return false
	}
}

func isEntityTrailingStopWord(term string) bool {
	switch term {
	case "lidero", "publico", "presento", "descubrio", "en", "del", "de", "la", "el", "los", "las":
		return true
	default:
		return false
	}
}

func containsWeakStructuralTerms(terms []string) bool {
	for _, term := range terms {
		if isInterrogativeOrConnector(term) {
			return true
		}
	}
	return false
}

func isStrongEntityTerm(term string) bool {
	if len([]rune(term)) >= 7 && !isGenericEntityTerm(term) && !isEntityQueryStopWord(term) && !isInterrogativeOrConnector(term) {
		return true
	}
	switch term {
	case "proxima", "centauri", "mercurio", "venus", "marte", "jupiter", "saturno", "neptuno", "pluton", "einstein", "newton", "galaxia", "estrella", "planeta", "orbita", "constelacion", "telescopio", "algoritmo", "arquitectura", "pipeline", "postgres", "ollama":
		return true
	default:
		return false
	}
}

func isSpecificEntitySuffix(term string) bool {
	if len([]rune(term)) == 1 {
		r := []rune(term)[0]
		return unicode.IsLetter(r)
	}
	return false
}

func hasProperLikeEntity(terms []string) bool {
	for _, term := range terms {
		if len([]rune(term)) >= 2 && !isEntityQueryStopWord(term) && !isGenericEntityTerm(term) && !isInterrogativeOrConnector(term) {
			hasDigit := false
			for _, r := range term {
				if unicode.IsDigit(r) {
					hasDigit = true
					break
				}
			}
			if hasDigit || len([]rune(term)) >= 8 {
				return true
			}
		}
	}
	return false
}

func shouldApplyEntityFilter(entity string) bool {
	entity = strings.TrimSpace(entity)
	if entity == "" || len([]rune(entity)) < minEntityFilterRunes {
		return false
	}
	terms := strings.Fields(entity)
	if len(terms) < 2 {
		return false
	}
	if hasWeakEntityTrailingBoundary(terms) {
		return false
	}
	if containsWeakEntityTerms(terms) && !hasClearlyProperRealEntity(terms) {
		return false
	}
	return true
}

func isWeakEntityTerm(term string) bool {
	switch term {
	case "segun", "documento", "tema", "parte", "informacion", "contenido":
		return true
	default:
		return false
	}
}

func containsWeakEntityTerms(terms []string) bool {
	for _, term := range terms {
		if isWeakEntityTerm(term) {
			return true
		}
	}
	return false
}

func hasWeakEntityTrailingBoundary(terms []string) bool {
	if len(terms) == 0 {
		return false
	}
	last := terms[len(terms)-1]
	return isWeakEntityTerm(last) || isInterrogativeOrConnector(last)
}

func hasClearlyProperRealEntity(terms []string) bool {
	meaningful := make([]string, 0, len(terms))
	specificCount := 0
	for _, term := range terms {
		if isWeakEntityTerm(term) || isEntityQueryStopWord(term) || isGenericEntityTerm(term) || isInterrogativeOrConnector(term) {
			continue
		}
		meaningful = append(meaningful, term)
		if termHasDigit(term) || isSpecificEntitySuffix(term) || len([]rune(term)) >= 7 || isStrongEntityTerm(term) {
			specificCount++
		}
	}
	if len(meaningful) < 2 {
		return false
	}
	if specificCount >= 2 {
		return true
	}
	return looksSpecificNamedEntity(meaningful) && hasProperLikeEntity(meaningful)
}

func termHasDigit(term string) bool {
	for _, r := range term {
		if unicode.IsDigit(r) {
			return true
		}
	}
	return false
}

func containsMainTerms(terms []string, content string) bool {
	if len(terms) == 0 {
		return false
	}
	normalizedContent := NormalizeSearchText(content)
	for _, term := range terms {
		if countWholeTerm(normalizedContent, term) > 0 || countApproximateTerm(normalizedContent, term) > 0 || hasPartialTermMatch(normalizedContent, term) {
			return true
		}
	}
	return false
}

func containsNormalizedPhrase(content string, normalizedPhrase string) bool {
	if normalizedPhrase == "" {
		return false
	}
	return strings.Contains(NormalizeSearchText(content), normalizedPhrase)
}

func hasPartialTermMatch(content string, term string) bool {
	termRunes := []rune(term)
	if len(termRunes) < 5 {
		return false
	}

	prefixLen := len(termRunes) - 1
	if prefixLen > 7 {
		prefixLen = 7
	}
	if prefixLen < 4 {
		return false
	}
	prefix := string(termRunes[:prefixLen])

	for _, word := range strings.Fields(content) {
		if strings.HasPrefix(word, prefix) || strings.Contains(word, term) || strings.Contains(term, word) && len([]rune(word)) >= 5 {
			return true
		}
	}
	return false
}

func lengthPenalty(query, content string, textScore float64) float64 {
	contentLen := len([]rune(content))
	matchIndex := firstStrongMatchIndex(query, content)
	if contentLen <= 700 {
		return 1
	}
	if containsExactPhrase(query, content) || textScore >= 0.85 {
		return 1
	}
	if contentLen > 1800 {
		if matchIndex > 1200 && textScore < 0.9 {
			return 0.42
		}
		return 0.62
	}
	if contentLen > 1200 {
		if matchIndex > 800 && textScore < 0.9 {
			return 0.55
		}
		return 0.74
	}
	if matchIndex > 600 && textScore < 0.7 {
		return 0.72
	}
	return 0.86
}

func relevantSnippet(query, content string) string {
	content = strings.TrimSpace(content)
	runes := []rune(content)
	if len(runes) <= maxResultContentRunes {
		return content
	}

	lowerContent := NormalizeSearchText(content)
	lowerQuery := NormalizeSearchText(query)
	queryIntent := detectQueryIntent(query)
	topicFocus := detectTopicFocus(query)
	index := -1
	if topicFocus.Enabled {
		for _, variant := range topicFocus.Variants {
			if variant = NormalizeSearchText(variant); variant != "" {
				index = strings.Index(lowerContent, variant)
				if index >= 0 {
					break
				}
			}
		}
	}
	if index < 0 && queryIntent.MainEntity != "" {
		index = strings.Index(lowerContent, queryIntent.MainEntity)
	}
	if index < 0 {
		index = strings.Index(lowerContent, lowerQuery)
	}
	if index < 0 {
		index = firstTermIndex(lowerContent, meaningfulTerms(lowerQuery))
	}
	if index < 0 {
		index = 0
	}

	start := index - maxResultContentRunes/3
	if start < 0 {
		start = 0
	}
	end := start + maxResultContentRunes
	if end > len(runes) {
		end = len(runes)
		start = end - maxResultContentRunes
		if start < 0 {
			start = 0
		}
	}

	snippet := strings.TrimSpace(string(runes[start:end]))
	if start > 0 {
		snippet = "..." + snippet
	}
	if end < len(runes) {
		snippet += "..."
	}
	return snippet
}

func firstTermIndex(content string, terms []string) int {
	best := -1
	for _, term := range terms {
		index := strings.Index(content, term)
		if index >= 0 && (best < 0 || index < best) {
			best = index
		}
	}
	return best
}

func containsExactPhrase(query, content string) bool {
	normalizedQuery := NormalizeSearchText(query)
	if normalizedQuery == "" {
		return false
	}
	return strings.Contains(NormalizeSearchText(content), normalizedQuery)
}

func isPrecisionQuery(query string) bool {
	terms := meaningfulTerms(NormalizeSearchText(query))
	return len(terms) >= 1 && len(terms) <= 3
}

func hasStrongQueryMatch(query string, content string) bool {
	normalizedQuery := NormalizeSearchText(query)
	normalizedContent := NormalizeSearchText(content)
	if normalizedQuery == "" || normalizedContent == "" {
		return false
	}
	if strings.Contains(normalizedContent, normalizedQuery) {
		return true
	}
	for _, term := range meaningfulTerms(normalizedQuery) {
		if countWholeTerm(normalizedContent, term) > 0 {
			return true
		}
	}
	if mainTerm := mainMeaningfulTerm(normalizedQuery); mainTerm != "" && countApproximateTerm(normalizedContent, mainTerm) > 0 {
		return true
	}
	return false
}

func isDefinitionLike(query string, content string) bool {
	normalizedContent := NormalizeSearchText(content)
	if normalizedContent == "" {
		return false
	}

	for _, term := range meaningfulTerms(NormalizeSearchText(query)) {
		patterns := []string{
			term + " es ",
			term + " son ",
			term + " se define ",
			term + " consiste ",
			term + " corresponde ",
			term + " permite ",
			term + " representa ",
			term + " hace referencia ",
			"se denomina " + term,
			"llamado " + term,
		}
		for _, pattern := range patterns {
			if strings.Contains(normalizedContent, pattern) {
				return true
			}
		}
	}

	return false
}

func genericSectionPenalty(query string, content string, precisionMode bool) float64 {
	if !isGenericSection(content) {
		return 1
	}
	if precisionMode || isConceptualQuery(query) {
		return 0.18
	}
	return 0.45
}

func isGenericSection(content string) bool {
	normalizedContent := NormalizeSearchText(content)
	genericPhrases := []string{
		"palabras clave",
		"preguntas",
		"preguntas de repaso",
		"guia de busqueda",
		"fuentes y referencias",
		"referencias",
	}
	for _, phrase := range genericPhrases {
		if strings.HasPrefix(normalizedContent, phrase) || strings.Contains(normalizedContent, phrase) {
			return true
		}
	}
	return false
}

func isConceptualQuery(query string) bool {
	normalizedQuery := NormalizeSearchText(query)
	if isPrecisionQuery(query) {
		return true
	}
	conceptTerms := []string{"que es", "definicion", "concepto", "significa", "para que sirve"}
	for _, term := range conceptTerms {
		if strings.Contains(normalizedQuery, term) {
			return true
		}
	}
	return false
}

func firstStrongMatchIndex(query string, content string) int {
	normalizedQuery := NormalizeSearchText(query)
	normalizedContent := NormalizeSearchText(content)
	if normalizedQuery == "" || normalizedContent == "" {
		return -1
	}
	if index := strings.Index(normalizedContent, normalizedQuery); index >= 0 {
		return index
	}
	return firstTermIndex(normalizedContent, meaningfulTerms(normalizedQuery))
}

func countWholeTerm(content string, term string) int {
	count := 0
	for _, word := range strings.Fields(content) {
		if word == term {
			count++
		}
	}
	return count
}

func countApproximateTerm(content string, term string) int {
	if len([]rune(term)) < 5 {
		return 0
	}

	count := 0
	maxDistance := fuzzyDistanceLimit(term)
	for _, word := range strings.Fields(content) {
		if approximateTermMatch(term, word, maxDistance) {
			count++
		}
	}
	return count
}

func approximateTermMatch(queryTerm string, contentTerm string, maxDistance int) bool {
	if queryTerm == "" || contentTerm == "" {
		return false
	}
	if queryTerm == contentTerm {
		return true
	}

	queryLen := len([]rune(queryTerm))
	contentLen := len([]rune(contentTerm))
	if queryLen < 5 || contentLen < 5 {
		return false
	}
	if absInt(queryLen-contentLen) > maxDistance {
		return false
	}

	return levenshteinWithin(queryTerm, contentTerm, maxDistance)
}

func fuzzyDistanceLimit(term string) int {
	length := len([]rune(term))
	if length >= 9 {
		return 2
	}
	return 1
}

func levenshteinWithin(a string, b string, maxDistance int) bool {
	ar := []rune(a)
	br := []rune(b)

	if absInt(len(ar)-len(br)) > maxDistance {
		return false
	}

	previous := make([]int, len(br)+1)
	current := make([]int, len(br)+1)
	for j := range previous {
		previous[j] = j
	}

	for i := 1; i <= len(ar); i++ {
		current[0] = i
		rowMin := current[0]
		for j := 1; j <= len(br); j++ {
			cost := 0
			if ar[i-1] != br[j-1] {
				cost = 1
			}
			current[j] = minInt(
				previous[j]+1,
				current[j-1]+1,
				previous[j-1]+cost,
			)
			if current[j] < rowMin {
				rowMin = current[j]
			}
		}
		if rowMin > maxDistance {
			return false
		}
		previous, current = current, previous
	}

	return previous[len(br)] <= maxDistance
}

func mainMeaningfulTerm(text string) string {
	terms := meaningfulTerms(text)
	if len(terms) == 0 {
		return ""
	}
	return terms[0]
}

func minInt(values ...int) int {
	best := values[0]
	for _, value := range values[1:] {
		if value < best {
			best = value
		}
	}
	return best
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

func isDuplicateResult(content string, existing []postgres.SearchResult) bool {
	contentTerms := uniqueTerms(NormalizeSearchText(content))
	if len(contentTerms) == 0 {
		return false
	}

	for _, item := range existing {
		otherTerms := uniqueTerms(NormalizeSearchText(item.Content))
		if jaccardSimilarity(contentTerms, otherTerms) >= duplicateThreshold {
			return true
		}
	}
	return false
}

func jaccardSimilarity(a map[string]bool, b map[string]bool) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}

	intersection := 0
	for term := range a {
		if b[term] {
			intersection++
		}
	}

	union := len(a) + len(b) - intersection
	if union == 0 {
		return 0
	}
	return float64(intersection) / float64(union)
}

func uniqueTerms(text string) map[string]bool {
	terms := make(map[string]bool)
	for _, term := range meaningfulTerms(text) {
		terms[term] = true
	}
	return terms
}

func meaningfulTerms(text string) []string {
	rawTerms := strings.Fields(text)
	terms := make([]string, 0, len(rawTerms))
	seen := make(map[string]bool)
	for _, term := range rawTerms {
		if len([]rune(term)) < 2 || isStopWord(term) || seen[term] {
			continue
		}
		seen[term] = true
		terms = append(terms, term)
	}
	return terms
}

func NormalizeSearchText(text string) string {
	text = strings.ToLower(strings.TrimSpace(text))
	text = strings.NewReplacer(
		"á", "a",
		"é", "e",
		"í", "i",
		"ó", "o",
		"ú", "u",
		"ü", "u",
		"ñ", "n",
	).Replace(text)
	text = norm.NFD.String(text)

	var builder strings.Builder
	lastSpace := true
	for _, r := range text {
		if unicode.Is(unicode.Mn, r) {
			continue
		}
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			builder.WriteRune(r)
			lastSpace = false
			continue
		}
		if !lastSpace {
			builder.WriteByte(' ')
			lastSpace = true
		}
	}
	return strings.TrimSpace(builder.String())
}

func NormalizeQuestionForRetrieval(question string) string {
	question = strings.TrimSpace(question)
	if question == "" {
		return ""
	}

	tails := []string{
		"segun el documento",
		"según el documento",
		"de acuerdo con el documento",
		"en el documento",
		"del documento",
	}

	cleaned := strings.TrimSpace(question)
	for {
		updated := false
		lower := strings.ToLower(strings.TrimSpace(cleaned))
		for _, tail := range tails {
			if strings.HasSuffix(lower, tail) {
				cleaned = strings.TrimSpace(cleaned[:len(cleaned)-len(tail)])
				cleaned = strings.TrimRight(cleaned, " ,.;:-")
				updated = true
				break
			}
			for _, prefix := range []string{", ", " ", " - ", ": "} {
				composite := prefix + tail
				if strings.HasSuffix(lower, composite) {
					cleaned = strings.TrimSpace(cleaned[:len(cleaned)-len(composite)])
					cleaned = strings.TrimRight(cleaned, " ,.;:-")
					updated = true
					break
				}
			}
			if updated {
				break
			}
		}
		if !updated {
			break
		}
	}

	cleaned = strings.TrimSpace(cleaned)
	if cleaned == "" {
		return strings.TrimSpace(question)
	}
	return cleaned
}

func normalizeText(text string) string {
	return NormalizeSearchText(text)
}

func isStopWord(term string) bool {
	switch term {
	case "a", "al", "el", "la", "los", "las", "de", "del", "que", "se", "en", "un", "una", "unos", "unas", "y", "o", "para", "por", "con", "su", "sus", "es":
		return true
	default:
		return false
	}
}
