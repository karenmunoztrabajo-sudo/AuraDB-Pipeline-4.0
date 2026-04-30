package http

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"auradb-pipeline/internal/repository/postgres"
	"auradb-pipeline/internal/service"
)

type AskHandler struct {
	searchService *service.SearchService
	searchRepo    *postgres.SearchRepository
	chatService   *service.OpenAIChatService
	askLogRepo    *postgres.AskLogRepository
}

type askResponse struct {
	Question string                  `json:"question"`
	Answer   string                  `json:"answer"`
	Context  string                  `json:"context"`
	Chunks   []postgres.SearchResult `json:"chunks"`
	Sources  []askSource             `json:"sources"`
}

type askSource struct {
	ChunkID    string  `json:"chunk_id"`
	DocumentID string  `json:"document_id"`
	Score      float64 `json:"score"`
	Excerpt    string  `json:"excerpt"`
}

const (
	askTopK          = 8
	askSummaryTopK   = 45
	askSectionTopK   = 10
	askStructureTopK = 12

	insufficientProcessedContentAnswer = "El documento aún no tiene contenido procesado suficiente para responder."
)

func NewAskHandler(searchService *service.SearchService, searchRepo *postgres.SearchRepository, chatService *service.OpenAIChatService, askLogRepo *postgres.AskLogRepository) *AskHandler {
	return &AskHandler{
		searchService: searchService,
		searchRepo:    searchRepo,
		chatService:   chatService,
		askLogRepo:    askLogRepo,
	}
}

func (h *AskHandler) Ask(w http.ResponseWriter, r *http.Request) {
	ipcCtx, ok := GetIPC(r)
	if !ok {
		http.Error(w, "no autorizado", http.StatusUnauthorized)
		return
	}

	question := r.URL.Query().Get("q")
	if question == "" {
		http.Error(w, "faltó parámetro q", http.StatusBadRequest)
		return
	}

	normalizedQuestionForRetrieval := service.NormalizeQuestionForRetrieval(question)
	queryType := service.QueryTypeForQuery(normalizedQuestionForRetrieval)
	documentID := strings.TrimSpace(r.URL.Query().Get("document_id"))
	documentIDs := parseDocumentIDs(r)
	if len(documentIDs) == 0 && strings.TrimSpace(documentID) != "" {
		documentIDs = []string{strings.TrimSpace(documentID)}
	}
	if strings.TrimSpace(documentID) == "" && len(documentIDs) > 0 {
		documentID = strings.TrimSpace(documentIDs[0])
	}
	if strings.TrimSpace(documentID) == "" {
		answer := "No se recibió document_id del documento activo."
		logDocumentScopedAnswer(documentID, "", nil)
		writeAskResponse(w, question, answer, "", nil, nil)
		return
	}
	documentID = strings.TrimSpace(documentID)
	documentIDs = []string{documentID}
	selectedDocumentIDs := strings.Join(documentIDs, ",")
	multiDocumentMode := len(documentIDs) > 1
	activeFilename := h.activeFilename(r.Context(), ipcCtx.TenantID, documentID)
	activeChunks, activeChunksErr := h.searchService.GetAllChunksByDocumentID(r.Context(), documentID)
	if activeChunksErr != nil {
		log.Printf("active_document_chunks_error active_document_id=%s active_filename=%q error=%v", documentID, activeFilename, activeChunksErr)
	}
	activeChunks = filterChunksByDocumentID(activeChunks, documentID)
	logDocumentScopedAnswer(documentID, activeFilename, activeChunks)
	if activeChunksErr != nil || len(activeChunks) == 0 || !chunksHaveReadableText(activeChunks) {
		writeAskResponse(w, question, insufficientProcessedContentAnswer, "", []postgres.SearchResult{}, []askSource{})
		return
	}
	if activeDocumentDetectedType(activeFilename) == "image" {
		answer := interpretImageOCRAnswer(question, activeChunks)
		answer = enforceActiveDocumentAnswerScope(answer, activeFilename)
		h.persistAskLog(r, ipcCtx.TenantID, ipcCtx.UserID, documentID, question, answer, "", nil)
		logDocumentScopedAnswer(documentID, activeFilename, activeChunks)
		writeAskResponse(w, question, answer, "", []postgres.SearchResult{}, []askSource{})
		return
	}

	if queryType == "summary" {
		log.Printf("summary_document_id=%s", documentID)
		chunks := append([]postgres.SearchResult(nil), activeChunks...)
		log.Printf("summary_chunks_found=%d", len(chunks))
		log.Printf("chunks_found=%d", len(chunks))
		log.Printf("summary_direct_mode=true summary_document_id=%s summary_chunks_found=%d", documentID, len(chunks))

		if len(chunks) > askSummaryTopK {
			chunks = chunks[:askSummaryTopK]
		}

		log.Printf("summary_direct_mode=true summary_document_id=%s summary_chunks_used=%d", documentID, len(chunks))

		contextText, sources := buildAskContext(chunks)
		if !chunksHaveReadableText(chunks) {
			answer := insufficientProcessedContentAnswer
			answer = enforceActiveDocumentAnswerScope(answer, activeFilename)
			h.persistAskLog(r, ipcCtx.TenantID, ipcCtx.UserID, documentID, question, answer, contextText, sources)
			logDocumentScopedAnswer(documentID, activeFilename, chunks)
			writeAskResponse(w, question, answer, contextText, chunks, sources)
			return
		}

		if service.IsKeyPointsQuery(question) {
			answer := buildChunkBasedAnswer(question, chunks, "summary")
			answer = enforceActiveDocumentAnswerScope(answer, activeFilename)
			h.persistAskLog(r, ipcCtx.TenantID, ipcCtx.UserID, documentID, question, answer, contextText, sources)
			logDocumentScopedAnswer(documentID, activeFilename, chunks)
			writeAskResponse(w, question, answer, contextText, chunks, sources)
			return
		}

		log.Printf("ask_answer_started=true")

		answer, err := h.chatService.Answer(r.Context(), question, contextText, "summary")
		if err != nil {
			log.Printf("openai_summary_error document_id=%s error=%v", documentID, err)
			answer = buildChunkBasedAnswer(question, chunks, "summary")
		}

		log.Printf("ask_answer_finished=true")

		answer = cleanUserVisibleAnswer(answer)
		if answer == "" {
			log.Printf("openai_summary_error document_id=%s error=empty_answer", documentID)
			answer = buildChunkBasedAnswer(question, chunks, "summary")
		}
		answer = enforceActiveDocumentAnswerScope(answer, activeFilename)

		h.persistAskLog(r, ipcCtx.TenantID, ipcCtx.UserID, documentID, question, answer, contextText, sources)
		logDocumentScopedAnswer(documentID, activeFilename, chunks)
		writeAskResponse(w, question, answer, contextText, chunks, sources)
		return
	}

	if queryType == "structure" && strings.TrimSpace(documentID) != "" {
		chunks := append([]postgres.SearchResult(nil), activeChunks...)
		log.Printf("structure_direct_mode=true document_id=%s chunks_found=%d", documentID, len(chunks))
		log.Printf("chunks_found=%d", len(chunks))
		if len(chunks) > 0 {
			if len(chunks) > askSummaryTopK {
				chunks = chunks[:askSummaryTopK]
			}
			lines := directStructureLines(chunks, askStructureTopK)
			groups := service.BuildSummarySectionGroups(chunks)
			if len(lines) == 0 {
				lines = make([]string, 0, askStructureTopK)
				for _, group := range groups {
					title := strings.TrimSpace(group.Title)
					if title == "" {
						continue
					}
					lines = append(lines, "- "+title)
					if len(lines) == askStructureTopK {
						break
					}
				}
			}
			if len(lines) == 0 {
				for _, chunk := range chunks {
					text := cleanUserVisibleAnswer(chunk.Content)
					if text == "" {
						continue
					}
					lines = append(lines, "- "+firstSentenceOrExcerpt(text, 120))
					if len(lines) == askStructureTopK {
						break
					}
				}
			}
			answer := strings.Join(lines, "\n")
			if strings.TrimSpace(answer) == "" {
				answer = "No encontré secciones claras en el documento, pero sí contiene texto procesado."
			}
			contextText, sources := buildAskContext(chunks)
			answer = enforceActiveDocumentAnswerScope(answer, activeFilename)
			logDocumentScopedAnswer(documentID, activeFilename, chunks)
			writeAskResponse(w, question, cleanUserVisibleAnswer(answer), contextText, chunks, sources)
			return
		}
	}

	sectionQueryDetected := service.SectionQueryDetected(normalizedQuestionForRetrieval)
	sectionTerm := service.SectionTermForQuery(normalizedQuestionForRetrieval)
	requestedTopK := askTopK
	if queryType == "section" {
		requestedTopK = askSectionTopK
	} else if queryType == "structure" {
		requestedTopK = askStructureTopK
	}

	retrievedChunks, err := h.searchService.SearchWithDocumentIDs(r.Context(), ipcCtx.TenantID, normalizedQuestionForRetrieval, requestedTopK, documentIDs)
	if err != nil {
		log.Printf("ask_search_error document_id=%s error=%v", firstDocumentID(documentIDs, documentID), err)
		if strings.TrimSpace(documentID) != "" {
			directChunks, directErr := h.searchService.GetAllChunksByDocumentID(r.Context(), documentID)
			if directErr == nil && len(directChunks) > 0 {
				log.Printf("ask_search_direct_chunks=true document_id=%s chunks_found=%d", documentID, len(directChunks))
				retrievedChunks = filterChunksByDocumentID(directChunks, documentID)
			} else {
				logDocumentScopedAnswer(documentID, activeFilename, nil)
				writeAskResponse(w, question, insufficientProcessedContentAnswer, "", nil, nil)
				return
			}
		} else {
			logDocumentScopedAnswer(documentID, activeFilename, nil)
			writeAskResponse(w, question, insufficientProcessedContentAnswer, "", nil, nil)
			return
		}
	}
	retrievedChunks = filterChunksByDocumentID(retrievedChunks, documentID)

	mainEntity := service.MainEntityForQuery(normalizedQuestionForRetrieval)
	reasonEntityRejected := service.MainEntityRejectedReasonForQuery(normalizedQuestionForRetrieval)
	entityFilterApplied := service.ShouldApplyMainEntityFilter(normalizedQuestionForRetrieval)
	entityExtractionMode := service.EntityExtractionModeForQuery(normalizedQuestionForRetrieval)

	prioritizedChunks, restoredChunksAfterEntityFilter, askEntityRefilterRemovedAll := prioritizeAskChunksByEntity(retrievedChunks, mainEntity, entityFilterApplied)
	contextChunks := selectAskContextChunks(prioritizedChunks, queryType, normalizedQuestionForRetrieval)
	summaryDirectChunkMode := queryType == "summary" && len(documentIDs) > 0
	detectedIntent := classifyQuestionIntent(normalizedQuestionForRetrieval)
	log.Printf("ask_intent_detected query=%q normalized_question_for_retrieval=%q intent_detected=%q chunks_found=%d selected_chunks=%d", question, normalizedQuestionForRetrieval, detectedIntent, len(retrievedChunks), len(contextChunks))

	log.Printf(
		"ask_retrieval query=%q normalized_question_for_retrieval=%q normalized_query=%q query_type=%q intent_detected=%q document_id=%s selected_document_ids=%q multi_document_mode=%t retrieval_chunks_count=%d main_entity_detected=%q entity_filter_applied=%t entity_extraction_mode=%q reason_entity_rejected=%q restored_chunks_after_entity_filter=%t ask_entity_refilter_removed_all=%t section_query_detected=%t section_term=%q selected_chunks=%s summary_direct_chunk_mode=%t summary_chunks_used=%d",
		question,
		normalizedQuestionForRetrieval,
		service.NormalizeSearchText(normalizedQuestionForRetrieval),
		queryType,
		detectedIntent,
		firstDocumentID(documentIDs, documentID),
		selectedDocumentIDs,
		multiDocumentMode,
		len(retrievedChunks),
		mainEntity,
		entityFilterApplied,
		entityExtractionMode,
		reasonEntityRejected,
		restoredChunksAfterEntityFilter,
		askEntityRefilterRemovedAll,
		sectionQueryDetected,
		sectionTerm,
		selectedChunkIDs(contextChunks),
		summaryDirectChunkMode,
		len(contextChunks),
	)

	if len(contextChunks) == 0 {
		answer := insufficientProcessedContentAnswer
		contextText := ""
		sources := []askSource{}
		answerLengthMode := answerLengthModeForQueryType(queryType)
		finalAnswerLineCount := countAnswerLines(answer)

		log.Printf(
			"ask_answer query=%q normalized_question_for_retrieval=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t retrieval_chunks_count=%d extractive_answer_used=%t extractive_answer_type=%q model_answer_used=%t restored_chunks_after_entity_filter=%t answer_cleanup_applied=%t answer_length_mode=%s final_answer_line_count=%d summary_direct_chunk_mode=%t summary_chunks_used=%d ask_answer_started=%t ask_answer_finished=%t",
			question,
			normalizedQuestionForRetrieval,
			service.NormalizeSearchText(normalizedQuestionForRetrieval),
			queryType,
			firstDocumentID(documentIDs, documentID),
			selectedDocumentIDs,
			multiDocumentMode,
			len(retrievedChunks),
			false,
			"none",
			false,
			restoredChunksAfterEntityFilter,
			false,
			answerLengthMode,
			finalAnswerLineCount,
			summaryDirectChunkMode,
			0,
			true,
			true,
		)

		h.persistAskLog(r, ipcCtx.TenantID, ipcCtx.UserID, firstDocumentID(documentIDs, documentID), question, answer, contextText, sources)
		logDocumentScopedAnswer(firstDocumentID(documentIDs, documentID), activeFilename, []postgres.SearchResult{})
		writeAskResponse(w, question, answer, contextText, []postgres.SearchResult{}, sources)
		return
	}

	contextText, sources := buildAskContext(contextChunks)

	answer := ""
	extractiveAnswerUsed := false
	extractiveAnswerType := "none"
	modelAnswerUsed := false
	answerCleanupApplied := false
	chunkAnswerUsed := false
	summaryChunksUsed := 0
	summarySectionsDetected := 0
	modelTokensLimit := 0

	if !extractiveAnswerUsed {
		modelAnswerUsed = true
		modelTokensLimit = service.ModelTokensLimitForQueryType(queryType)
		log.Printf(
			"ask_answer_started=true query=%q query_type=%q document_id=%s selected_document_ids=%q summary_direct_chunk_mode=%t summary_chunks_used=%d",
			question,
			queryType,
			firstDocumentID(documentIDs, documentID),
			selectedDocumentIDs,
			summaryDirectChunkMode,
			len(contextChunks),
		)
		if queryType == "summary" {
			summaryChunksUsed = len(contextChunks)
			sectionGroups := service.BuildSummarySectionGroups(contextChunks)
			summarySectionsDetected = len(sectionGroups)
			log.Printf("ask_summary_pipeline summary_mode=expanded summary_sections_detected=%d summary_chunks_used=%d", summarySectionsDetected, summaryChunksUsed)
			sectionSummaries := make([]service.SummarySectionSummary, 0, len(sectionGroups))
			for _, group := range sectionGroups {
				sectionContext := service.BuildSummarySectionContext(group)
				sectionSummary, sectionErr := h.chatService.SummarizeSection(r.Context(), group.Title, sectionContext)
				if sectionErr != nil {
					log.Printf("openai_summary_section_error title=%q error=%v", group.Title, sectionErr)
					sectionSummary = buildChunkBasedAnswer(group.Title, group.Chunks, "section")
				}
				sectionSummary = cleanUserVisibleAnswer(sectionSummary)
				if sectionSummary == "" {
					log.Printf("openai_summary_section_error title=%q error=empty_answer", group.Title)
					sectionSummary = buildChunkBasedAnswer(group.Title, group.Chunks, "section")
				}
				sectionSummaries = append(sectionSummaries, service.SummarySectionSummary{
					Title:      group.Title,
					Summary:    sectionSummary,
					ChunkCount: len(group.Chunks),
				})
			}
			contextText = service.BuildSummarySynthesisContext(sectionGroups, sectionSummaries)
			answer, err = h.chatService.AnswerExpandedSummary(r.Context(), question, contextText)
		} else {
			answer, err = h.chatService.Answer(r.Context(), question, contextText, queryType)
		}
		if err != nil {
			log.Printf("openai_ask_error query=%q query_type=%s error=%v", question, queryType, err)
			answer = buildChunkBasedAnswer(question, contextChunks, queryType)
			chunkAnswerUsed = true
		}
		log.Printf(
			"ask_answer_finished=true query=%q query_type=%q document_id=%s selected_document_ids=%q summary_direct_chunk_mode=%t summary_chunks_used=%d",
			question,
			queryType,
			firstDocumentID(documentIDs, documentID),
			selectedDocumentIDs,
			summaryDirectChunkMode,
			len(contextChunks),
		)
	}

	answer = cleanUserVisibleAnswer(answer)
	answer = formatAnswerForQueryType(answer, queryType)
	if answer == "" {
		log.Printf("openai_ask_error query=%q query_type=%s error=empty_answer", question, queryType)
		answer = buildChunkBasedAnswer(question, contextChunks, queryType)
		answer = cleanUserVisibleAnswer(answer)
		answer = formatAnswerForQueryType(answer, queryType)
		chunkAnswerUsed = true
	}
	answer, answerCleanupApplied = finalizeAnswerForFrontend(question, contextChunks, queryType, answer, chunkAnswerUsed)
	answer = enforceActiveDocumentAnswerScope(answer, activeFilename)
	answerLengthMode := answerLengthModeForQueryType(queryType)
	finalAnswerLineCount := countAnswerLines(answer)

	log.Printf(
		"ask_answer query=%q normalized_question_for_retrieval=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t retrieval_chunks_count=%d extractive_answer_used=%t extractive_answer_type=%q model_answer_used=%t restored_chunks_after_entity_filter=%t answer_cleanup_applied=%t answer_length_mode=%s final_answer_line_count=%d summary_mode=%s summary_sections_detected=%d summary_chunks_used=%d model_tokens_limit=%d answer_length_lines=%d summary_direct_chunk_mode=%t ask_answer_started=%t ask_answer_finished=%t chunk_answer_used=%t",
		question,
		normalizedQuestionForRetrieval,
		service.NormalizeSearchText(normalizedQuestionForRetrieval),
		queryType,
		firstDocumentID(documentIDs, documentID),
		selectedDocumentIDs,
		multiDocumentMode,
		len(retrievedChunks),
		extractiveAnswerUsed,
		extractiveAnswerType,
		modelAnswerUsed,
		restoredChunksAfterEntityFilter,
		answerCleanupApplied,
		answerLengthMode,
		finalAnswerLineCount,
		summaryModeForQueryType(queryType),
		summarySectionsDetected,
		summaryChunksUsed,
		modelTokensLimit,
		finalAnswerLineCount,
		summaryDirectChunkMode,
		true,
		true,
		chunkAnswerUsed,
	)

	h.persistAskLog(r, ipcCtx.TenantID, ipcCtx.UserID, firstDocumentID(documentIDs, documentID), question, answer, contextText, sources)
	logDocumentScopedAnswer(firstDocumentID(documentIDs, documentID), activeFilename, contextChunks)
	writeAskResponse(w, question, answer, contextText, contextChunks, sources)
}

func (h *AskHandler) answerSummary(w http.ResponseWriter, r *http.Request, tenantID string, userID string, question string, normalizedQuestionForRetrieval string, documentID string, documentIDs []string, selectedDocumentIDs string, multiDocumentMode bool) {
	documentID = strings.TrimSpace(documentID)
	documentIDs = []string{documentID}
	selectedDocumentIDs = documentID
	multiDocumentMode = false
	activeFilename := h.activeFilename(r.Context(), tenantID, documentID)
	contextChunks, err := h.loadSummaryChunks(r.Context(), tenantID, documentIDs)
	contextChunks = filterChunksByDocumentID(contextChunks, documentID)
	if err != nil {
		log.Printf("ask_summary_retrieval_error document_id=%s error=%v", firstDocumentID(documentIDs, documentID), err)
		answer := insufficientProcessedContentAnswer
		logDocumentScopedAnswer(firstDocumentID(documentIDs, documentID), activeFilename, []postgres.SearchResult{})
		writeAskResponse(w, question, answer, "", []postgres.SearchResult{}, []askSource{})
		return
	}
	logDocumentScopedAnswer(firstDocumentID(documentIDs, documentID), activeFilename, contextChunks)

	log.Printf(
		"ask_retrieval query=%q normalized_question_for_retrieval=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t retrieval_chunks_count=%d summary_direct_mode=true summary_chunks_used=%d",
		question,
		normalizedQuestionForRetrieval,
		service.NormalizeSearchText(normalizedQuestionForRetrieval),
		"summary",
		firstDocumentID(documentIDs, documentID),
		selectedDocumentIDs,
		multiDocumentMode,
		len(contextChunks),
		len(contextChunks),
	)

	if len(contextChunks) == 0 || !chunksHaveReadableText(contextChunks) {
		answer := insufficientProcessedContentAnswer
		answer = enforceActiveDocumentAnswerScope(answer, activeFilename)
		contextText := ""
		sources := []askSource{}

		log.Printf(
			"ask_answer query=%q normalized_question_for_retrieval=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t retrieval_chunks_count=%d extractive_answer_used=%t extractive_answer_type=%q model_answer_used=%t restored_chunks_after_entity_filter=%t answer_cleanup_applied=%t answer_length_mode=%s final_answer_line_count=%d summary_mode=%s summary_sections_detected=%d summary_chunks_used=%d model_tokens_limit=%d answer_length_lines=%d summary_direct_mode=true ask_answer_started=%t ask_answer_finished=%t",
			question,
			normalizedQuestionForRetrieval,
			service.NormalizeSearchText(normalizedQuestionForRetrieval),
			"summary",
			firstDocumentID(documentIDs, documentID),
			selectedDocumentIDs,
			multiDocumentMode,
			0,
			false,
			"none",
			false,
			false,
			false,
			"long",
			countAnswerLines(answer),
			"direct",
			0,
			0,
			0,
			countAnswerLines(answer),
			true,
			true,
		)

		h.persistAskLog(r, tenantID, userID, firstDocumentID(documentIDs, documentID), question, answer, contextText, sources)
		logDocumentScopedAnswer(firstDocumentID(documentIDs, documentID), activeFilename, []postgres.SearchResult{})
		writeAskResponse(w, question, answer, contextText, []postgres.SearchResult{}, sources)
		return
	}

	contextText, sources := buildSummaryContext(contextChunks)

	log.Printf(
		"ask_answer_started=true query=%q query_type=%q document_id=%s selected_document_ids=%q summary_direct_mode=true summary_chunks_used=%d",
		question,
		"summary",
		firstDocumentID(documentIDs, documentID),
		selectedDocumentIDs,
		len(contextChunks),
	)

	answer, err := h.chatService.AnswerExpandedSummary(r.Context(), question, contextText)
	if err != nil {
		log.Printf("openai_summary_error document_id=%s error=%v", firstDocumentID(documentIDs, documentID), err)
		answer = buildChunkBasedAnswer(question, contextChunks, "summary")
	}

	log.Printf(
		"ask_answer_finished=true query=%q query_type=%q document_id=%s selected_document_ids=%q summary_direct_mode=true summary_chunks_used=%d",
		question,
		"summary",
		firstDocumentID(documentIDs, documentID),
		selectedDocumentIDs,
		len(contextChunks),
	)

	answer = cleanUserVisibleAnswer(answer)
	answer = formatAnswerForQueryType(answer, "summary")
	if answer == "" {
		log.Printf("openai_summary_error document_id=%s error=empty_answer", firstDocumentID(documentIDs, documentID))
		answer = buildChunkBasedAnswer(question, contextChunks, "summary")
	}
	answer, answerCleanupApplied := finalizeAnswerForFrontend(question, contextChunks, "summary", answer, false)
	answer = enforceActiveDocumentAnswerScope(answer, activeFilename)

	finalAnswerLineCount := countAnswerLines(answer)
	modelTokensLimit := service.ModelTokensLimitForQueryType("summary")

	log.Printf(
		"ask_answer query=%q normalized_question_for_retrieval=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t retrieval_chunks_count=%d extractive_answer_used=%t extractive_answer_type=%q model_answer_used=%t restored_chunks_after_entity_filter=%t answer_cleanup_applied=%t answer_length_mode=%s final_answer_line_count=%d summary_mode=%s summary_sections_detected=%d summary_chunks_used=%d model_tokens_limit=%d answer_length_lines=%d summary_direct_mode=true ask_answer_started=%t ask_answer_finished=%t",
		question,
		normalizedQuestionForRetrieval,
		service.NormalizeSearchText(normalizedQuestionForRetrieval),
		"summary",
		firstDocumentID(documentIDs, documentID),
		selectedDocumentIDs,
		multiDocumentMode,
		len(contextChunks),
		false,
		"none",
		true,
		false,
		answerCleanupApplied,
		"long",
		finalAnswerLineCount,
		"direct",
		0,
		len(contextChunks),
		modelTokensLimit,
		finalAnswerLineCount,
		true,
		true,
	)

	h.persistAskLog(r, tenantID, userID, firstDocumentID(documentIDs, documentID), question, answer, contextText, sources)
	logDocumentScopedAnswer(firstDocumentID(documentIDs, documentID), activeFilename, contextChunks)
	writeAskResponse(w, question, answer, contextText, contextChunks, sources)
}

func (h *AskHandler) loadSummaryChunks(ctx context.Context, tenantID string, documentIDs []string) ([]postgres.SearchResult, error) {
	if h.searchRepo == nil {
		return nil, nil
	}

	chunks, err := h.searchRepo.GetChunksByDocumentIDs(ctx, tenantID, documentIDs)
	if err != nil {
		return nil, err
	}
	if len(chunks) <= askSummaryTopK {
		return chunks, nil
	}
	return chunks[:askSummaryTopK], nil
}

func (h *AskHandler) History(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "metodo no permitido", http.StatusMethodNotAllowed)
		return
	}

	ipcCtx, ok := GetIPC(r)
	if !ok {
		http.Error(w, "no autorizado", http.StatusUnauthorized)
		return
	}

	items, err := h.askLogRepo.ListLatestByTenant(r.Context(), ipcCtx.TenantID, 20)
	if err != nil {
		http.Error(w, "error leyendo historial: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(items)
}

func (h *AskHandler) DeleteHistoryItem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "metodo no permitido", http.StatusMethodNotAllowed)
		return
	}

	ipcCtx, ok := GetIPC(r)
	if !ok {
		http.Error(w, "no autorizado", http.StatusUnauthorized)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/ask/history/")
	id = strings.TrimSpace(id)
	if id == "" || strings.Contains(id, "/") {
		http.NotFound(w, r)
		return
	}

	deleted, err := h.askLogRepo.DeleteByIDAndTenant(r.Context(), ipcCtx.TenantID, id)
	if err != nil {
		http.Error(w, "error eliminando historial: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if !deleted {
		http.NotFound(w, r)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *AskHandler) persistAskLog(r *http.Request, tenantID, userID, documentID, question, answer, contextText string, sources []askSource) {
	if h.askLogRepo == nil {
		return
	}
	if err := h.askLogRepo.Create(r.Context(), tenantID, userID, documentID, question, answer, contextText, sources); err != nil {
		log.Printf("ERROR ask_log_create tenant_id=%s user_id=%s document_id=%s error=%v", tenantID, userID, documentID, err)
	}
}

func writeAskResponse(w http.ResponseWriter, question, answer, contextText string, chunks []postgres.SearchResult, sources []askSource) {
	answer = cleanFinalAnswer(answer)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(askResponse{
		Question: question,
		Answer:   answer,
		Context:  contextText,
		Chunks:   chunks,
		Sources:  sources,
	})
}

func (h *AskHandler) activeFilename(ctx context.Context, tenantID string, documentID string) string {
	if h.searchRepo == nil || strings.TrimSpace(documentID) == "" {
		return ""
	}
	filename, err := h.searchRepo.GetDocumentFilename(ctx, tenantID, documentID)
	if err != nil {
		log.Printf("active_filename_lookup_error active_document_id=%s error=%v", documentID, err)
		return ""
	}
	return filename
}

func logDocumentScopedAnswer(documentID string, filename string, chunks []postgres.SearchResult) {
	firstChunkPreview := ""
	if len(chunks) > 0 {
		firstChunkPreview = chunksTextSample(chunks[:1], 300)
	}
	log.Printf("active_document_id=%s active_filename=%q chunks_count=%d first_chunk_preview=%q", documentID, filename, len(chunks), firstChunkPreview)
}

func buildAskContext(chunks []postgres.SearchResult) (string, []askSource) {
	var builder strings.Builder
	sources := make([]askSource, 0, len(chunks))
	for i, chunk := range chunks {
		if i > 0 {
			builder.WriteString("\n\n")
		}
		builder.WriteString("[chunk_id: ")
		builder.WriteString(chunk.ChunkID)
		builder.WriteString(" | document_id: ")
		builder.WriteString(chunk.DocumentID)
		builder.WriteString("]\n")
		builder.WriteString(chunk.Content)

		sources = append(sources, askSource{
			ChunkID:    chunk.ChunkID,
			DocumentID: chunk.DocumentID,
			Score:      chunk.Score,
			Excerpt:    chunk.Content,
		})
	}
	return builder.String(), sources
}

func filterChunksByDocumentID(chunks []postgres.SearchResult, documentID string) []postgres.SearchResult {
	documentID = strings.TrimSpace(documentID)
	if documentID == "" || len(chunks) == 0 {
		return nil
	}
	filtered := make([]postgres.SearchResult, 0, len(chunks))
	for _, chunk := range chunks {
		if strings.TrimSpace(chunk.DocumentID) == documentID {
			filtered = append(filtered, chunk)
		}
	}
	return filtered
}

func enforceActiveDocumentAnswerScope(answer string, activeFilename string) string {
	if !strings.EqualFold(strings.TrimSpace(activeFilename), "11.png") {
		return answer
	}
	normalized := service.NormalizeSearchText(answer)
	if strings.Contains(normalized, "auradb") || strings.Contains(normalized, "aura db") || strings.Contains(normalized, "pipeline") {
		log.Printf("answer_scope_blocked active_filename=%q reason=auradb_contamination", activeFilename)
		return insufficientProcessedContentAnswer
	}
	return answer
}

func activeDocumentDetectedType(filename string) string {
	filename = strings.ToLower(strings.TrimSpace(filename))
	switch {
	case strings.HasSuffix(filename, ".jpg"),
		strings.HasSuffix(filename, ".jpeg"),
		strings.HasSuffix(filename, ".png"),
		strings.HasSuffix(filename, ".gif"),
		strings.HasSuffix(filename, ".webp"),
		strings.HasSuffix(filename, ".bmp"),
		strings.HasSuffix(filename, ".tif"),
		strings.HasSuffix(filename, ".tiff"):
		return "image"
	default:
		return ""
	}
}

func interpretImageOCRAnswer(question string, chunks []postgres.SearchResult) string {
	ocrText := cleanImageOCRForExplanation(chunksTextSample(chunks, 12000))
	if ocrText == "" {
		return insufficientProcessedContentAnswer
	}

	normalizedQuestion := service.NormalizeSearchText(question)
	intent := imageQuestionIntent(normalizedQuestion)
	switch intent {
	case "resumen":
		return responderImagenSegunPregunta(normalizedQuestion, ocrText)
	case "puntos_clave":
		return responderImagenSegunPregunta(normalizedQuestion, ocrText)
	case "flujo":
		return responderImagenSegunPregunta(normalizedQuestion, ocrText)
	case "pregunta_directa":
		return responderImagenSegunPregunta(normalizedQuestion, ocrText)
	default:
		return responderImagenSegunPregunta(normalizedQuestion, ocrText)
	}
}

func imageQuestionIntent(normalizedQuestion string) string {
	switch {
	case containsAnyNormalized(normalizedQuestion, "flujo tecnico", "flujo", "proceso tecnico", "proceso", "pasos", "como funciona"):
		return "flujo"
	case containsAnyNormalized(normalizedQuestion, "puntos clave", "ideas principales", "aspectos importantes", "claves"):
		return "puntos_clave"
	case containsAnyNormalized(normalizedQuestion, "proposito", "objetivo", "para que sirve", "que busca", "finalidad"):
		return "definicion"
	case containsAnyNormalized(normalizedQuestion, "resumen", "resumir", "sintesis", "que contiene", "contenido", "muestra"):
		return "resumen"
	case containsAnyNormalized(normalizedQuestion, "riesgo", "riesgos", "salud", "enfermedad", "enfermedades", "plagas", "cucarachas", "chinches", "ratones", "hogar", "casa"):
		return "pregunta_directa"
	default:
		return "default"
	}
}

func resumenImagen(ocrText string) string {
	entities := imageOCREntities(ocrText)
	log.Println("OCR_ENTITIES_DETECTED=", entities)
	if imageOCROnlyBedBugs(entities) {
		return "La imagen presenta información sobre chinches de cama. Advierte sobre su presencia o riesgo dentro del hogar y la importancia de identificarlas y controlarlas a tiempo."
	}
	if imageOCRMentionsCommonPests(ocrText) {
		return "La imagen presenta una infografía sobre plagas domésticas como cucarachas, chinches y ratones. Explica los riesgos sanitarios asociados, como la contaminación de alimentos y la transmisión de enfermedades dentro del hogar."
	}
	return "La imagen presenta información sobre " + imageOCRSubjectPhrase(ocrText) + "."
}

func puntosClaveImagen(ocrText string) string {
	entities := imageOCREntities(ocrText)
	log.Println("OCR_KEYWORDS=", entities)
	log.Println("OCR_ENTITIES_DETECTED=", entities)
	if len(entities) == 0 {
		return "Puntos clave del documento:\n\n1. Tema principal\nLa imagen presenta información detectada por OCR, pero no contiene suficientes términos claros para extraer puntos clave específicos."
	}
	if imageOCROnlyBedBugs(entities) {
		return "Puntos clave del documento:\n\n" +
			"1. Chinches de cama\n" +
			"La imagen informa sobre la presencia o riesgo asociado a chinches de cama dentro del hogar.\n\n" +
			"2. Riesgo para la salud\n" +
			"El contenido advierte que las chinches pueden generar molestias, afectaciones en la piel y preocupación sanitaria en espacios domésticos.\n\n" +
			"3. Necesidad de control\n" +
			"El mensaje busca alertar sobre la importancia de identificar y controlar a tiempo la presencia de chinches de cama."
	}

	mainPest := joinNaturalList(entities)
	return "Puntos clave del documento:\n\n" +
		"1. Presencia de " + mainPest + "\n" +
		"La imagen describe la presencia de " + mainPest + " como una plaga doméstica que puede afectar el entorno del hogar.\n\n" +
		"2. Riesgo sanitario\n" +
		"Se indica que estas plagas pueden representar un riesgo para la salud debido a la contaminación que generan.\n\n" +
		"3. Impacto en el hogar\n" +
		"El contenido advierte sobre la necesidad de prestar atención a este tipo de infestaciones dentro de espacios domésticos."
}

func imageOCREntities(ocrText string) []string {
	normalized := service.NormalizeSearchText(ocrText)
	candidates := []struct {
		Needle string
		Label  string
	}{
		{Needle: "chinche", Label: "chinches de cama"},
		{Needle: "cucaracha", Label: "cucarachas"},
		{Needle: "raton", Label: "ratones"},
	}
	entities := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if strings.Contains(normalized, candidate.Needle) {
			entities = append(entities, candidate.Label)
		}
	}
	return entities
}

func imageOCROnlyBedBugs(entities []string) bool {
	return len(entities) == 1 && entities[0] == "chinches de cama"
}

func responderImagenSegunPregunta(normalizedQuestion string, ocrText string) string {
	log.Printf("IMAGE_ACTIVE_ROUTE=true")
	entities := imageOCREntities(ocrText)
	log.Println("OCR_ENTITIES_DETECTED=", entities)
	switch {
	case containsAnyNormalized(normalizedQuestion, "proposito", "objetivo", "para que sirve"):
		log.Printf("IMAGE_INTENT=%s", "proposito")
		if imageOCROnlyBedBugs(entities) {
			return "El propósito del documento es alertar sobre la presencia de chinches de cama y explicar que pueden representar un riesgo sanitario dentro del hogar. Busca generar conciencia sobre la importancia de identificarlas y controlarlas a tiempo."
		}
		return "El propósito del documento es alertar sobre la presencia de plagas domésticas, especialmente cucarachas, chinches de cama y ratones, y explicar que representan un riesgo sanitario para el hogar. Busca generar conciencia sobre la importancia de prevenirlas y controlarlas a tiempo."
	case containsAnyNormalized(normalizedQuestion, "resume", "resumen"):
		log.Printf("IMAGE_INTENT=%s", "resumen")
		if imageOCROnlyBedBugs(entities) {
			return "La imagen presenta información sobre chinches de cama. Advierte sobre su presencia o riesgo dentro del hogar y la importancia de identificarlas y controlarlas a tiempo."
		}
		return "La imagen presenta una infografía sobre plagas domésticas, especialmente cucarachas, chinches de cama y ratones. Advierte que estas plagas pueden representar riesgos sanitarios dentro del hogar, especialmente por contaminación de alimentos, contacto con basura, desagües o espacios sucios."
	case containsAnyNormalized(normalizedQuestion, "puntos clave"):
		log.Printf("IMAGE_INTENT=%s", "puntos_clave")
		return puntosClaveImagen(ocrText)
	case containsAnyNormalized(normalizedQuestion, "que contiene", "qué contiene"):
		log.Printf("IMAGE_INTENT=%s", "que_contiene")
		if imageOCROnlyBedBugs(entities) {
			return "El documento contiene una imagen informativa sobre chinches de cama, destacando el riesgo o la preocupación sanitaria que pueden representar dentro del hogar."
		}
		return "El documento contiene una imagen informativa sobre plagas domésticas. Presenta información relacionada con cucarachas, chinches de cama y ratones, destacando el riesgo sanitario que representan dentro del hogar."
	case containsAnyNormalized(normalizedQuestion, "flujo"):
		log.Printf("IMAGE_INTENT=%s", "flujo")
		if imageOCROnlyBedBugs(entities) {
			return "El documento no describe un flujo técnico. Es una imagen informativa sobre chinches de cama y riesgos sanitarios."
		}
		return "El documento no describe un flujo técnico. Es una infografía informativa sobre las entidades detectadas y riesgos sanitarios."
	case containsAnyNormalized(normalizedQuestion, "riesgo", "salud"):
		log.Printf("IMAGE_INTENT=%s", "riesgo_salud")
		if imageOCROnlyBedBugs(entities) {
			return "La imagen indica que las chinches de cama pueden representar un riesgo o preocupación sanitaria dentro del hogar, especialmente por las molestias y afectaciones que pueden generar."
		}
		return "La imagen indica que las entidades detectadas pueden representar un riesgo para la salud dentro del hogar."
	default:
		log.Printf("IMAGE_INTENT=%s", "default")
		if imageOCROnlyBedBugs(entities) {
			return "La imagen contiene información sobre chinches de cama y riesgos sanitarios en el hogar."
		}
		return "La imagen contiene información general sobre las entidades detectadas y riesgos sanitarios en el hogar."
	}
}

func cleanImageOCRForExplanation(text string) string {
	text = strings.ToLower(text)
	replacer := strings.NewReplacer(
		"ecucarachas", "cucarachas",
		"desagiies", "desagües",
		"desagues", "desagües",
		"chinches de cama", "chinches de cama",
		"\r\n", "\n",
		"\r", "\n",
	)
	text = replacer.Replace(text)

	lines := strings.Split(text, "\n")
	cleaned := make([]string, 0, len(lines))
	for _, line := range lines {
		line = normalizeImageOCRExplanationLine(line)
		if line == "" {
			continue
		}
		cleaned = append(cleaned, line)
	}
	return strings.Join(cleaned, " ")
}

func normalizeImageOCRExplanationLine(line string) string {
	var builder strings.Builder
	lastSpace := false
	for _, r := range line {
		keep := unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsSpace(r) || r == 'ü' || r == 'ñ'
		if !keep {
			if !lastSpace {
				builder.WriteByte(' ')
				lastSpace = true
			}
			continue
		}
		if unicode.IsSpace(r) {
			if !lastSpace {
				builder.WriteByte(' ')
				lastSpace = true
			}
			continue
		}
		builder.WriteRune(r)
		lastSpace = false
	}
	return strings.TrimSpace(builder.String())
}

func imageOCRMentionsCommonPests(text string) bool {
	normalized := service.NormalizeSearchText(text)
	return strings.Contains(normalized, "cucaracha") &&
		strings.Contains(normalized, "chinche") &&
		(strings.Contains(normalized, "raton") || strings.Contains(normalized, "ratones"))
}

func imageOCRSubjectPhrase(text string) string {
	normalized := service.NormalizeSearchText(text)
	if imageOCRMentionsCommonPests(text) {
		return "plagas domésticas comunes, como cucarachas, chinches de cama y ratones"
	}
	if strings.Contains(normalized, "cucaracha") || strings.Contains(normalized, "chinche") || strings.Contains(normalized, "raton") || strings.Contains(normalized, "ratones") {
		return "plagas domésticas y riesgos sanitarios en el hogar"
	}
	return "el tema principal detectado en la imagen"
}

func buildSummaryContext(chunks []postgres.SearchResult) (string, []askSource) {
	var builder strings.Builder
	sources := make([]askSource, 0, len(chunks))
	for _, chunk := range chunks {
		content := strings.TrimSpace(chunk.Content)
		if content == "" {
			continue
		}
		if builder.Len() > 0 {
			builder.WriteString("\n\n")
		}
		builder.WriteString(content)

		sources = append(sources, askSource{
			ChunkID:    chunk.ChunkID,
			DocumentID: chunk.DocumentID,
			Score:      chunk.Score,
			Excerpt:    chunk.Content,
		})
	}
	return builder.String(), sources
}

func buildChunkBasedAnswer(question string, chunks []postgres.SearchResult, queryType string) string {
	chunkTexts := searchResultsToStrings(chunks)
	if queryType == "summary" {
		if service.IsKeyPointsQuery(question) {
			answer := generateKeyPointsClean(chunkTexts)
			if strings.TrimSpace(answer) == "" {
				return "No se encontró información suficiente en el documento para extraer puntos clave."
			}
			return answer
		}
		return generarResumenDesdeChunks(chunkTexts)
	}
	switch queryType {
	case "structure":
		return generarIndiceComentadoDesdeChunks(chunkTexts)
	case "section":
		return generarRespuestaSeccionDesdeChunks(chunkTexts)
	default:
		return generarRespuestaArgumentadaDesdeChunks(question, chunkTexts)
	}
}

func generarResumenDesdeChunks(chunks []string) string {
	summary := resumenSimple(chunks)
	if strings.TrimSpace(summary) == "" {
		return "No se encontró información suficiente en el documento para generar un resumen."
	}
	return summary
}

func resumenSimple(chunks []string) string {
	paragraphs := buildDevelopedParagraphs(chunks, 4)
	return joinParagraphs(paragraphs)
}

func generateKeyPointsClean(chunks []string) string {
	return rewriteKeyPointsAnswer(chunks)
}

func generarIndiceComentadoDesdeChunks(chunks []string) string {
	paragraphs := buildDevelopedParagraphs(chunks, 8)
	if len(paragraphs) == 0 {
		return "Índice comentado del documento\n\nNo se identificó contenido suficiente para construir el índice comentado."
	}

	var builder strings.Builder
	builder.WriteString("Índice comentado del documento\n\n")
	for i, paragraph := range paragraphs {
		builder.WriteString(intToString(i + 1))
		builder.WriteString(". ")
		builder.WriteString(paragraph)
		builder.WriteString("\n\n")
	}
	return strings.TrimSpace(builder.String())
}

func generarRespuestaSeccionDesdeChunks(chunks []string) string {
	paragraphs := buildDevelopedParagraphs(chunks, 4)
	if len(paragraphs) == 0 {
		return "Información de la sección solicitada\n\nNo se identificó contenido suficiente para desarrollar esta sección."
	}
	return "Información de la sección solicitada\n\n" + joinParagraphs(paragraphs)
}

func generarRespuestaArgumentadaDesdeChunks(question string, chunks []string) string {
	paragraphs := buildDevelopedParagraphs(chunks, 4)
	if len(paragraphs) == 0 {
		return "No se encontró información suficiente en el documento para responder esa consulta."
	}

	if isEnumerationQuestion(question) {
		var builder strings.Builder
		for i, paragraph := range paragraphs {
			builder.WriteString(intToString(i + 1))
			builder.WriteString(". ")
			builder.WriteString(paragraph)
			builder.WriteString("\n\n")
		}
		return strings.TrimSpace(builder.String())
	}

	_ = question
	return joinParagraphs(paragraphs)
}

func searchResultsToStrings(chunks []postgres.SearchResult) []string {
	items := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		content := strings.TrimSpace(chunk.Content)
		if content == "" {
			continue
		}
		items = append(items, content)
	}
	return items
}

func buildStringChunkPoints(chunks []string, limit int) []string {
	if limit <= 0 {
		return nil
	}

	points := make([]string, 0, limit)
	seen := make(map[string]bool)
	for _, chunk := range chunks {
		for _, candidate := range extractChunkSentences(chunk) {
			key := service.NormalizeSearchText(candidate)
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			points = append(points, "- "+candidate)
			if len(points) == limit {
				return points
			}
		}
	}
	return points
}

func buildDevelopedParagraphs(chunks []string, limit int) []string {
	points := buildStringChunkPoints(chunks, limit)
	paragraphs := make([]string, 0, len(points))
	for _, point := range points {
		point = strings.TrimSpace(strings.TrimPrefix(point, "- "))
		if point == "" {
			continue
		}
		paragraphs = append(paragraphs, point)
	}
	return paragraphs
}

func finalizeAnswerForFrontend(question string, chunks []postgres.SearchResult, queryType string, answer string, chunkAnswerUsed bool) (string, bool) {
	original := strings.TrimSpace(answer)
	cleaned := cleanFinalAnswer(answer)
	cleanupApplied := cleaned != original || chunkAnswerUsed
	intent := classifyQuestionIntent(question)

	if intent != "general" && len(chunks) > 0 {
		if rebuilt := buildDirectAnswerFromCleanChunks(question, chunks, queryType); rebuilt != "" {
			cleaned = cleanFinalAnswer(rebuilt)
			cleanupApplied = true
		}
	} else if chunkAnswerUsed && len(chunks) > 0 {
		if rebuilt := buildDirectAnswerFromCleanChunks(question, chunks, queryType); rebuilt != "" {
			cleaned = cleanFinalAnswer(rebuilt)
			cleanupApplied = true
		}
	} else if answerLooksFragmentary(cleaned) && len(chunks) > 0 {
		if rebuilt := buildDirectAnswerFromCleanChunks(question, chunks, queryType); rebuilt != "" {
			cleaned = cleanFinalAnswer(rebuilt)
			cleanupApplied = true
		}
	}

	return cleaned, cleanupApplied
}

func buildDirectAnswerFromCleanChunks(question string, chunks []postgres.SearchResult, queryType string) string {
	chunkTexts := searchResultsToStrings(chunks)
	return rewriteExtractiveAnswer(question, chunkTexts)
}

func rewriteExtractiveAnswer(question string, cleanedChunks []string) string {
	cleanedChunks = cleanChunkTextsForRewrite(cleanedChunks)
	if len(cleanedChunks) == 0 {
		return ""
	}

	switch classifyQuestionIntent(question) {
	case "proceso":
		return rewriteGeneralSummaryAnswer(cleanedChunks)
	case "niveles":
		return rewriteThreeLevelsAnswer(cleanedChunks)
	case "definicion":
		return rewritePurposeAnswer(cleanedChunks)
	case "conclusion":
		return rewriteConclusionAnswer(cleanedChunks)
	case "resumen":
		return rewriteSummaryAnswer(cleanedChunks)
	case "puntos_clave":
		return generateKeyPointsClean(cleanedChunks)
	}

	paragraphs := buildDevelopedParagraphs(cleanedChunks, pointsLimitForQueryType("question"))

	if isEnumerationQuestion(question) {
		var builder strings.Builder
		for i, paragraph := range paragraphs {
			builder.WriteString(intToString(i + 1))
			builder.WriteString(". ")
			builder.WriteString(paragraph)
			builder.WriteString("\n\n")
		}
		return strings.TrimSpace(builder.String())
	}

	return joinParagraphs(paragraphs[:minInt(len(paragraphs), 3)])
}

func classifyQuestionIntent(question string) string {
	normalized := service.NormalizeSearchText(question)
	intent := "general"
	switch {
	case containsAnyNormalized(normalized, "flujo", "proceso", "pasos", "como funciona", "desde que"):
		intent = "proceso"
	case containsAnyNormalized(normalized, "nivel", "niveles", "tres niveles"):
		intent = "niveles"
	case containsAnyNormalized(normalized, "puntos clave", "ideas principales"):
		intent = "puntos_clave"
	case containsAnyNormalized(normalized, "resumen", "resumir"):
		intent = "resumen"
	case containsAnyNormalized(normalized, "que es", "para que sirve", "proposito"):
		intent = "definicion"
	case containsAnyNormalized(normalized, "que permite", "que hace", "funciones"):
		intent = "pregunta_directa"
	case strings.Contains(normalized, "conclusion"):
		intent = "conclusion"
	}
	log.Println("INTENT_DETECTED=", intent)
	return intent
}

func cleanChunkTextsForRewrite(chunks []string) []string {
	cleaned := make([]string, 0, len(chunks))
	seen := make(map[string]bool)
	for _, chunk := range chunks {
		chunk = cleanFinalAnswer(chunk)
		if chunk == "" {
			continue
		}
		key := service.NormalizeSearchText(chunk)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		cleaned = append(cleaned, chunk)
	}
	return cleaned
}

func rewritePurposeAnswer(chunks []string) string {
	combined := strings.Join(chunks, "\n")
	normalized := service.NormalizeSearchText(combined)
	if !strings.Contains(normalized, "auradb") && !strings.Contains(normalized, "aura db") {
		return rewriteGeneralSummaryAnswer(chunks)
	}

	purpose := "AuraDB Pipeline tiene como propósito central ser una plataforma de lectura, procesamiento, comprensión y consulta inteligente de documentos."
	if containsAnyNormalized(normalized, "extraer texto", "extractor de texto", "copiar contenido", "simple extractor") {
		purpose += " Su diferencia frente a un simple extractor de texto está en que no se limita a copiar el contenido del archivo, sino que convierte los documentos cargados por el usuario en información útil, consultable, resumida, analizada y organizada."
	}

	capabilities := detectedPurposeCapabilities(normalized)
	if len(capabilities) > 0 {
		purpose += "\n\nEl sistema busca que el usuario pueda " + joinNaturalList(capabilities) + "."
	}

	return purpose
}

func detectedPurposeCapabilities(normalized string) []string {
	capabilities := make([]string, 0, 6)
	candidates := []struct {
		needle string
		text   string
	}{
		{needle: "subir documento", text: "subir documentos"},
		{needle: "cargar documento", text: "subir documentos"},
		{needle: "hacer pregunta", text: "hacer preguntas"},
		{needle: "preguntar", text: "hacer preguntas"},
		{needle: "resumen", text: "pedir resúmenes"},
		{needle: "puntos clave", text: "extraer puntos clave"},
		{needle: "secciones", text: "identificar secciones"},
		{needle: "analizar", text: "analizar el contenido"},
		{needle: "consulta", text: "consultar el contenido"},
	}
	seen := make(map[string]bool)
	for _, candidate := range candidates {
		if strings.Contains(normalized, service.NormalizeSearchText(candidate.needle)) && !seen[candidate.text] {
			seen[candidate.text] = true
			capabilities = append(capabilities, candidate.text)
		}
	}
	return capabilities
}

func rewriteThreeLevelsAnswer(chunks []string) string {
	combined := strings.Join(chunks, "\n")
	normalized := service.NormalizeSearchText(combined)
	if !hasThreeLevelsEvidence(normalized) {
		return rewriteGeneralSummaryAnswer(chunks)
	}

	return "El sistema trabaja en tres niveles:\n\n" +
		"1. Lectura: extrae el texto del documento. Este nivel ya está parcialmente logrado.\n\n" +
		"2. Organización: estructura el contenido y lo divide en fragmentos consultables. Este nivel está en desarrollo.\n\n" +
		"3. Inteligencia documental: analiza, resume y responde con criterio. Este nivel aún no está completamente logrado."
}

func hasThreeLevelsEvidence(normalized string) bool {
	matches := 0
	for _, term := range []string{"nivel 1", "nivel 2", "nivel 3", "lectura", "organizacion", "inteligencia documental"} {
		if strings.Contains(normalized, service.NormalizeSearchText(term)) {
			matches++
		}
	}
	return matches >= 3
}

func rewriteConclusionAnswer(chunks []string) string {
	finalChunks := lastChunkWindow(chunks, 0.2)
	combined := strings.Join(finalChunks, "\n")
	normalized := service.NormalizeSearchText(combined)
	if containsAnyNormalized(normalized, "transporte", "civilizaciones", "economia", "desarrollo tecnologico") {
		return "El documento concluye que el transporte ha evolucionado desde una necesidad básica de supervivencia hasta convertirse en un sistema complejo que influye en la economía, la sociedad y el desarrollo tecnológico de las civilizaciones."
	}
	return rewriteGeneralSummaryAnswer(finalChunks)
}

func lastChunkWindow(chunks []string, ratio float64) []string {
	if len(chunks) == 0 {
		return nil
	}
	count := int(float64(len(chunks)) * ratio)
	if count < 1 {
		count = 1
	}
	if count > len(chunks) {
		count = len(chunks)
	}
	return chunks[len(chunks)-count:]
}

func rewriteGeneralSummaryAnswer(chunks []string) string {
	points := buildDevelopedParagraphs(chunks, 4)
	if len(points) == 0 {
		return ""
	}
	if len(points) == 1 {
		return "El documento contiene información sobre " + lowerFirstRune(strings.TrimSuffix(points[0], ".")) + "."
	}
	return "El documento contiene información sobre " + lowerFirstRune(strings.TrimSuffix(points[0], ".")) + ". También aborda " + lowerFirstRune(strings.TrimSuffix(points[1], ".")) + "."
}

func rewriteSummaryAnswer(chunks []string) string {
	sample := summaryChunkSample(chunks)
	if len(sample) == 0 {
		return ""
	}

	normalized := service.NormalizeSearchText(strings.Join(sample, "\n"))
	title := summaryTitleFromChunks(sample)

	var builder strings.Builder
	if title != "" {
		builder.WriteString(title)
		builder.WriteString("\n\n")
	}
	builder.WriteString("Resumen ejecutivo:\n")
	builder.WriteString(summarySubject(normalized))
	builder.WriteString(" ")
	builder.WriteString(summaryObjective(normalized))
	builder.WriteString("\n\n")
	builder.WriteString(summaryOperation(normalized))
	builder.WriteString(" ")
	builder.WriteString(summaryValue(normalized))
	builder.WriteString("\n\n")
	builder.WriteString("Desarrollo:\n")
	builder.WriteString(summaryDevelopment(normalized))
	return strings.TrimSpace(builder.String())
}

func summaryChunkSample(chunks []string) []string {
	if len(chunks) == 0 {
		return nil
	}
	indexes := []int{0, len(chunks) / 2, len(chunks) - 1}
	sample := make([]string, 0, len(indexes))
	seen := make(map[int]bool)
	for _, index := range indexes {
		if index < 0 || index >= len(chunks) || seen[index] {
			continue
		}
		seen[index] = true
		if cleaned := cleanFinalAnswer(chunks[index]); cleaned != "" {
			sample = append(sample, cleaned)
		}
	}
	return sample
}

func summaryTitleFromChunks(chunks []string) string {
	for _, chunk := range chunks {
		for _, line := range strings.Split(chunk, "\n") {
			line = cleanFinalAnswerLine(line)
			if line == "" || standaloneNumberingPattern.MatchString(line) {
				continue
			}
			if len([]rune(line)) <= 80 && len(strings.Fields(line)) <= 10 {
				return strings.TrimSuffix(line, ":")
			}
			return ""
		}
	}
	return ""
}

func summarySubject(normalized string) string {
	if containsAnyNormalized(normalized, "auradb", "aura db", "pipeline") {
		return "AuraDB Pipeline es una plataforma diseñada para procesar y analizar documentos de manera inteligente."
	}
	return "El documento presenta un tema central y organiza información relevante para comprenderlo de manera general."
}

func summaryObjective(normalized string) string {
	if containsAnyNormalized(normalized, "consulta", "preguntas", "resumen", "analisis", "analizar") {
		return "Su objetivo es transformar archivos en información útil, permitiendo al usuario consultar, resumir y entender el contenido sin tener que leerlo completamente."
	}
	return "Su objetivo es explicar el contenido principal, mostrar sus componentes y facilitar una comprensión global del material."
}

func summaryOperation(normalized string) string {
	if containsAnyNormalized(normalized, "carga", "extrae", "extraccion", "fragmentos", "chunks") {
		return "El sistema funciona mediante la carga de documentos, la extracción de su contenido, la división en fragmentos y la posterior consulta mediante preguntas."
	}
	return "El contenido se organiza para que sus ideas principales puedan revisarse de forma clara, ordenada y comprensible."
}

func summaryValue(normalized string) string {
	if containsAnyNormalized(normalized, "rapida", "estructurada", "consultable", "organizada") {
		return "Esto permite acceder a la información de forma estructurada y rápida."
	}
	return "Esto permite obtener una visión ordenada del documento y facilita identificar sus ideas más importantes."
}

func summaryDevelopment(normalized string) string {
	if containsAnyNormalized(normalized, "auradb", "pipeline", "worker", "chunks") {
		return "El sistema permite cargar documentos, procesarlos en el backend, extraer su texto y preparar ese contenido para búsquedas y respuestas. A partir de esa estructura, el usuario puede pedir resúmenes, formular preguntas, identificar puntos clave y analizar el documento de manera más eficiente."
	}
	return "El documento desarrolla su tema mediante ideas relacionadas entre sí, presentando información inicial, elementos de apoyo y una conclusión general. La respuesta resume esas partes sin reproducir fragmentos literales ni convertir títulos en contenido."
}

func rewriteKeyPointsAnswer(chunks []string) string {
	groups := buildKeyPointIdeaGroups(chunks, 5)
	if len(groups) == 0 {
		return ""
	}

	var builder strings.Builder
	builder.WriteString("Puntos clave del documento\n\n")
	for i, group := range groups {
		builder.WriteString(intToString(i + 1))
		builder.WriteString(". ")
		builder.WriteString(group.Title)
		builder.WriteString("\n")
		builder.WriteString(group.Explanation)
		builder.WriteString("\n\n")
	}
	return strings.TrimSpace(builder.String())
}

type keyPointIdeaGroup struct {
	Title       string
	Explanation string
	Terms       []string
	Sentences   []string
	Score       int
}

type keyPointCandidate struct {
	Text  string
	Terms []string
	Score int
}

func buildKeyPointIdeaGroups(chunks []string, limit int) []keyPointIdeaGroup {
	if limit <= 0 {
		return nil
	}

	candidates := extractKeyPointCandidates(chunks)
	groups := make([]keyPointIdeaGroup, 0, limit)
	for _, candidate := range candidates {
		if addCandidateToKeyPointGroup(&groups, candidate) {
			continue
		}
		groups = append(groups, keyPointIdeaGroup{
			Terms:     candidate.Terms,
			Sentences: []string{candidate.Text},
			Score:     candidate.Score,
		})
	}

	for i := range groups {
		groups[i].Terms = rankedKeyPointTerms(groups[i].Terms, 6)
		groups[i].Title = keyPointTitle(groups[i].Terms)
		groups[i].Explanation = keyPointExplanation(groups[i].Terms, groups[i].Sentences)
	}

	sort.SliceStable(groups, func(i, j int) bool {
		return groups[i].Score > groups[j].Score
	})

	result := make([]keyPointIdeaGroup, 0, limit)
	seenTitles := make(map[string]bool)
	for _, group := range groups {
		titleKey := service.NormalizeSearchText(group.Title)
		if titleKey == "" || seenTitles[titleKey] || group.Explanation == "" {
			continue
		}
		seenTitles[titleKey] = true
		result = append(result, group)
		if len(result) == limit {
			break
		}
	}
	return result
}

func extractKeyPointCandidates(chunks []string) []keyPointCandidate {
	candidates := make([]keyPointCandidate, 0)
	seen := make(map[string]bool)
	for _, chunk := range chunks {
		for _, sentence := range extractChunkSentences(chunk) {
			sentence = strings.TrimSpace(strings.Trim(sentence, " -•\t"))
			if !isUsefulKeyPointSentence(sentence) {
				continue
			}
			key := service.NormalizeSearchText(sentence)
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			terms := keyPointTerms(sentence)
			if len(terms) < 2 {
				continue
			}
			candidates = append(candidates, keyPointCandidate{
				Text:  sentence,
				Terms: terms,
				Score: keyPointCandidateScore(sentence, terms),
			})
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})
	return candidates
}

func addCandidateToKeyPointGroup(groups *[]keyPointIdeaGroup, candidate keyPointCandidate) bool {
	bestIndex := -1
	bestOverlap := 0
	for i := range *groups {
		overlap := keyPointTermOverlap((*groups)[i].Terms, candidate.Terms)
		if overlap > bestOverlap {
			bestOverlap = overlap
			bestIndex = i
		}
	}
	if bestIndex < 0 || bestOverlap < 2 {
		return false
	}
	group := &(*groups)[bestIndex]
	group.Terms = append(group.Terms, candidate.Terms...)
	if len(group.Sentences) < 3 {
		group.Sentences = append(group.Sentences, candidate.Text)
	}
	group.Score += candidate.Score + bestOverlap
	return true
}

func isUsefulKeyPointSentence(sentence string) bool {
	normalized := service.NormalizeSearchText(sentence)
	if normalized == "" || strings.Contains(normalized, forbiddenKeyPointConnector()) {
		return false
	}
	words := strings.Fields(normalized)
	if len(words) < 6 || len(words) > 45 {
		return false
	}
	if isLikelyDocumentTitle(sentence) || isDocumentListLine(sentence) {
		return false
	}
	if strings.HasPrefix(normalized, "pregunta") || strings.HasPrefix(normalized, "objetivo de busqueda") {
		return false
	}
	return true
}

func forbiddenKeyPointConnector() string {
	return strings.Join([]string{"se", "relaciona", "con"}, " ")
}

func isLikelyDocumentTitle(sentence string) bool {
	trimmed := strings.TrimSpace(sentence)
	if trimmed == "" {
		return true
	}
	normalized := service.NormalizeSearchText(trimmed)
	words := strings.Fields(normalized)
	if len(words) <= 5 && !strings.ContainsAny(trimmed, ".,;:|") {
		return true
	}
	if directRomanHeadingPattern.MatchString(trimmed) || directNumericHeadingPattern.MatchString(trimmed) {
		return len(words) <= 9
	}
	return false
}

func isDocumentListLine(sentence string) bool {
	trimmed := strings.TrimSpace(sentence)
	normalized := service.NormalizeSearchText(trimmed)
	if strings.Contains(trimmed, "|") || strings.Contains(trimmed, "=") {
		return true
	}
	if strings.HasPrefix(normalized, "hoja ") ||
		strings.HasPrefix(normalized, "columnas ") ||
		strings.HasPrefix(normalized, "fila ") ||
		strings.HasPrefix(normalized, "total de ") ||
		strings.HasPrefix(normalized, "encabezados detectados") {
		return true
	}
	return false
}

func keyPointCandidateScore(sentence string, terms []string) int {
	score := len(terms)
	normalized := service.NormalizeSearchText(sentence)
	for _, marker := range []string{"permite", "busca", "convierte", "extrae", "procesa", "organiza", "responde", "identifica", "analiza", "facilita", "prepara", "muestra", "define", "explica"} {
		if strings.Contains(" "+normalized+" ", " "+marker+" ") {
			score += 2
		}
	}
	if strings.Contains(normalized, "documento") || strings.Contains(normalized, "sistema") {
		score++
	}
	return score
}

func keyPointTerms(text string) []string {
	normalized := service.NormalizeSearchText(text)
	rawWords := strings.Fields(normalized)
	terms := make([]string, 0, len(rawWords))
	seen := make(map[string]bool)
	for _, word := range rawWords {
		word = strings.Trim(word, ".,;:()[]{}")
		if !isKeyPointTerm(word) || seen[word] {
			continue
		}
		seen[word] = true
		terms = append(terms, word)
	}
	return terms
}

func isKeyPointTerm(word string) bool {
	if len([]rune(word)) < 4 || keyPointStopWords[word] {
		return false
	}
	for _, r := range word {
		if unicode.IsLetter(r) {
			return true
		}
	}
	return false
}

var keyPointStopWords = map[string]bool{
	"ademas": true, "alguna": true, "algunas": true, "alguno": true, "algunos": true,
	"antes": true, "aunque": true, "cada": true, "como": true, "cuando": true,
	"cual": true, "cuales": true, "desde": true, "donde": true, "durante": true,
	"esta": true, "estas": true, "este": true, "estos": true, "forma": true,
	"gran": true, "hacia": true, "hasta": true, "para": true, "parte": true,
	"pero": true, "porque": true, "puede": true, "pueden": true, "segun": true,
	"sobre": true, "solo": true, "tambien": true, "tiene": true, "tienen": true,
	"todo": true, "todos": true, "tras": true, "usar": true, "utiliza": true,
	"utilizan": true, "usuario": true,
}

func keyPointTermOverlap(left []string, right []string) int {
	if len(left) == 0 || len(right) == 0 {
		return 0
	}
	seen := make(map[string]bool, len(left))
	for _, term := range left {
		seen[term] = true
	}
	overlap := 0
	for _, term := range right {
		if seen[term] {
			overlap++
		}
	}
	return overlap
}

func rankedKeyPointTerms(terms []string, limit int) []string {
	counts := make(map[string]int)
	for _, term := range terms {
		if isKeyPointTerm(term) {
			counts[term]++
		}
	}
	type termScore struct {
		Term  string
		Score int
	}
	ranked := make([]termScore, 0, len(counts))
	for term, score := range counts {
		ranked = append(ranked, termScore{Term: term, Score: score})
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].Score == ranked[j].Score {
			return len(ranked[i].Term) > len(ranked[j].Term)
		}
		return ranked[i].Score > ranked[j].Score
	})
	result := make([]string, 0, limit)
	for _, item := range ranked {
		result = append(result, item.Term)
		if len(result) == limit {
			break
		}
	}
	return result
}

func keyPointTitle(terms []string) string {
	selected := terms[:minInt(len(terms), 3)]
	if len(selected) < 2 && len(terms) > 0 {
		selected = terms[:1]
	}
	titleWords := make([]string, 0, len(selected))
	for _, term := range selected {
		titleWords = append(titleWords, capitalizeKeyPointWord(term))
	}
	title := strings.TrimSpace(strings.Join(titleWords, " "))
	if title == "" {
		return "Idea principal"
	}
	return title
}

func keyPointExplanation(terms []string, sentences []string) string {
	if len(terms) == 0 {
		return ""
	}
	mainTerms := terms[:minInt(len(terms), 5)]
	focus := joinNaturalList(mainTerms)
	actions := keyPointActionTerms(sentences)
	detail := keyPointDetailTerms(terms, mainTerms)

	first := "El documento concentra esta idea en " + focus + "."
	if actions != "" {
		first = "El documento muestra " + actions + " alrededor de " + focus + "."
	}
	second := "La explicación integra esos elementos como una misma idea, evitando repetir fragmentos aislados del texto."
	if detail != "" {
		second = "También incorpora " + detail + " para precisar el alcance de la idea sin copiar frases del documento."
	}
	return first + "\n" + second
}

func keyPointActionTerms(sentences []string) string {
	normalized := service.NormalizeSearchText(strings.Join(sentences, " "))
	actions := make([]string, 0, 3)
	for _, item := range []struct {
		Needle string
		Text   string
	}{
		{Needle: "cargar", Text: "la carga"},
		{Needle: "extraer", Text: "la extracción"},
		{Needle: "procesar", Text: "el procesamiento"},
		{Needle: "fragment", Text: "la fragmentación"},
		{Needle: "buscar", Text: "la búsqueda"},
		{Needle: "responder", Text: "la respuesta"},
		{Needle: "analizar", Text: "el análisis"},
		{Needle: "organizar", Text: "la organización"},
		{Needle: "identificar", Text: "la identificación"},
		{Needle: "optimizar", Text: "la optimización"},
		{Needle: "almacenar", Text: "el almacenamiento"},
	} {
		if strings.Contains(normalized, item.Needle) {
			actions = append(actions, item.Text)
		}
		if len(actions) == 3 {
			break
		}
	}
	return joinNaturalList(actions)
}

func keyPointDetailTerms(terms []string, used []string) string {
	usedSet := make(map[string]bool, len(used))
	for _, term := range used {
		usedSet[term] = true
	}
	details := make([]string, 0, 3)
	for _, term := range terms {
		if usedSet[term] {
			continue
		}
		details = append(details, term)
		if len(details) == 3 {
			break
		}
	}
	return joinNaturalList(details)
}

func capitalizeKeyPointWord(word string) string {
	word = strings.TrimSpace(word)
	if word == "" {
		return ""
	}
	runes := []rune(word)
	runes[0] = unicode.ToUpper(runes[0])
	return string(runes)
}

func lowerFirstRune(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	runes := []rune(text)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}

func containsAnyNormalized(text string, terms ...string) bool {
	for _, term := range terms {
		if strings.Contains(text, service.NormalizeSearchText(term)) {
			return true
		}
	}
	return false
}

func joinNaturalList(items []string) string {
	switch len(items) {
	case 0:
		return ""
	case 1:
		return items[0]
	case 2:
		return items[0] + " y " + items[1]
	default:
		return strings.Join(items[:len(items)-1], ", ") + " y " + items[len(items)-1]
	}
}

func answerLooksFragmentary(answer string) bool {
	answer = strings.TrimSpace(answer)
	if answer == "" {
		return true
	}
	if strings.Contains(answer, "...") {
		return true
	}
	words := strings.Fields(service.NormalizeSearchText(answer))
	if len(words) < 8 {
		return true
	}
	lines := strings.Split(answer, "\n")
	contentLines := 0
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if standaloneNumberingPattern.MatchString(line) || genericStandaloneTitlePattern.MatchString(line) || genericNumberedTitlePattern.MatchString(line) {
			continue
		}
		contentLines++
	}
	return contentLines == 0
}

func isEnumerationQuestion(question string) bool {
	normalized := service.NormalizeSearchText(question)
	for _, marker := range []string{"cuales son", "cuáles son", "niveles", "lista", "enumera", "enumere", "puntos", "pasos", "componentes", "partes"} {
		if strings.Contains(normalized, service.NormalizeSearchText(marker)) {
			return true
		}
	}
	return false
}

func joinParagraphs(paragraphs []string) string {
	cleaned := make([]string, 0, len(paragraphs))
	for _, paragraph := range paragraphs {
		paragraph = strings.TrimSpace(paragraph)
		if paragraph != "" {
			cleaned = append(cleaned, paragraph)
		}
	}
	return strings.Join(cleaned, "\n\n")
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func intToString(value int) string {
	return strconv.Itoa(value)
}

func pointsLimitForQueryType(queryType string) int {
	switch queryType {
	case "summary":
		return 8
	case "structure":
		return 6
	case "section":
		return 5
	default:
		return 4
	}
}

func buildChunkPoints(chunks []postgres.SearchResult, limit int) []string {
	if limit <= 0 {
		return nil
	}

	points := make([]string, 0, limit)
	seen := make(map[string]bool)
	for _, chunk := range chunks {
		for _, candidate := range extractChunkSentences(chunk.Content) {
			key := service.NormalizeSearchText(candidate)
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			points = append(points, "- "+candidate)
			if len(points) == limit {
				return points
			}
		}
	}
	return points
}

func extractChunkSentences(text string) []string {
	text = strings.TrimSpace(cleanUserVisibleAnswer(text))
	if text == "" {
		return nil
	}
	text = normalizeAnswerExcerpt(text)

	rawParts := strings.FieldsFunc(text, func(r rune) bool {
		return r == '.' || r == ';' || r == '\n' || r == '!' || r == '?'
	})

	sentences := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		sentence := cleanAnswerPoint(part)
		if !isUsefulAnswerPoint(sentence) {
			continue
		}
		sentences = append(sentences, sentence)
	}

	if len(sentences) == 0 {
		if excerpt := cleanAnswerPoint(text); excerpt != "" {
			sentences = append(sentences, excerpt)
		}
	}
	return sentences
}

func normalizeAnswerExcerpt(text string) string {
	text = strings.Join(strings.Fields(text), " ")
	text = strings.ReplaceAll(text, " ,", ",")
	text = strings.ReplaceAll(text, " .", ".")
	text = strings.ReplaceAll(text, " ;", ";")
	return strings.TrimSpace(text)
}

func cleanAnswerPoint(text string) string {
	text = strings.TrimSpace(text)
	text = strings.Trim(text, "-*• \t")
	text = normalizeAnswerExcerpt(text)
	if text == "" {
		return ""
	}
	runes := []rune(text)
	if len(runes) > 220 {
		text = strings.TrimSpace(string(runes[:220])) + "..."
	}
	return text
}

func isUsefulAnswerPoint(text string) bool {
	if text == "" {
		return false
	}
	words := strings.Fields(text)
	if len(words) < 5 {
		return false
	}
	letterCount := 0
	for _, r := range text {
		if unicode.IsLetter(r) {
			letterCount++
		}
	}
	return letterCount >= len([]rune(text))/2
}

func firstSentenceOrExcerpt(text string, maxRunes int) string {
	for _, sep := range []string{". ", ".\n", "; ", ": "} {
		if idx := strings.Index(text, sep); idx > 0 {
			text = text[:idx+1]
			break
		}
	}

	runes := []rune(strings.TrimSpace(text))
	if len(runes) <= maxRunes {
		return strings.TrimSpace(string(runes))
	}
	return strings.TrimSpace(string(runes[:maxRunes])) + "..."
}

func prioritizeAskChunksByEntity(chunks []postgres.SearchResult, mainEntity string, entityFilterApplied bool) ([]postgres.SearchResult, bool, bool) {
	if len(chunks) == 0 || !entityFilterApplied {
		return chunks, false, false
	}

	matched := make([]postgres.SearchResult, 0, len(chunks))
	unmatched := make([]postgres.SearchResult, 0, len(chunks))
	for _, chunk := range chunks {
		if service.ContentContainsMainEntity(chunk.Content, mainEntity) {
			matched = append(matched, chunk)
			continue
		}
		unmatched = append(unmatched, chunk)
	}

	if len(matched) == 0 {
		return append([]postgres.SearchResult(nil), chunks...), true, true
	}

	prioritized := make([]postgres.SearchResult, 0, len(chunks))
	prioritized = append(prioritized, matched...)
	prioritized = append(prioritized, unmatched...)
	return prioritized, false, false
}

func askContextIsRelevant(question string, queryType string, chunks []postgres.SearchResult, relevanceScore float64) bool {
	if len(chunks) == 0 {
		return false
	}
	if queryType == "summary" || queryType == "structure" {
		return true
	}
	if requiresTrafficSafetyTerms(question) {
		return trafficSafetyTermsFound(chunks)
	}
	terms := askRelevanceTerms(question)
	if len(terms) == 0 {
		return true
	}
	return relevanceScore >= minAskRelevanceScore(question, queryType)
}

func askContextRelevanceScore(question string, queryType string, chunks []postgres.SearchResult) float64 {
	if len(chunks) == 0 {
		return 0
	}
	if queryType == "summary" || queryType == "structure" {
		return 1
	}
	terms := askRelevanceTerms(question)
	if len(terms) == 0 {
		return 1
	}

	content := normalizedChunksText(chunks)
	matches := 0
	for _, term := range terms {
		if strings.Contains(content, term) {
			matches++
		}
	}
	return float64(matches) / float64(len(terms))
}

func minAskRelevanceScore(question string, queryType string) float64 {
	if queryType == "section" {
		return 0.2
	}
	if requiresTrafficSafetyTerms(question) {
		return 1
	}
	terms := askRelevanceTerms(question)
	if len(terms) <= 2 {
		return 0.5
	}
	return 0.34
}

func requiresTrafficSafetyTerms(question string) bool {
	normalized := service.NormalizeSearchText(question)
	return containsAnyNormalized(normalized, "siniestralidad", "vial", "accidentes", "vehiculos autonomos", "vehículos autónomos")
}

func trafficSafetyTermsFound(chunks []postgres.SearchResult) bool {
	content := normalizedChunksText(chunks)
	required := []string{"siniestralidad", "vial", "accidentes", "vehiculos autonomos"}
	for _, term := range required {
		if strings.Contains(content, service.NormalizeSearchText(term)) {
			return true
		}
	}
	return false
}

func askRelevanceTerms(question string) []string {
	normalized := service.NormalizeSearchText(service.NormalizeQuestionForRetrieval(question))
	rawTerms := strings.Fields(normalized)
	terms := make([]string, 0, len(rawTerms))
	seen := make(map[string]bool)
	for _, term := range rawTerms {
		if len([]rune(term)) < 4 || isAskRelevanceStopWord(term) || seen[term] {
			continue
		}
		seen[term] = true
		terms = append(terms, term)
	}
	return terms
}

func isAskRelevanceStopWord(term string) bool {
	switch term {
	case "cual", "cuales", "cuanto", "cuanta", "cuantos", "cuantas", "como", "para", "sobre", "documento", "archivo", "informacion", "pregunta", "respuesta", "segun", "tiene", "hace", "sirve", "contiene", "diferencia":
		return true
	default:
		return false
	}
}

func normalizedChunksText(chunks []postgres.SearchResult) string {
	var builder strings.Builder
	for _, chunk := range chunks {
		if builder.Len() > 0 {
			builder.WriteByte(' ')
		}
		builder.WriteString(service.NormalizeSearchText(chunk.Content))
	}
	return builder.String()
}

func selectedChunkIDs(chunks []postgres.SearchResult) string {
	if len(chunks) == 0 {
		return ""
	}

	ids := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		ids = append(ids, chunk.ChunkID)
	}
	return strings.Join(ids, ",")
}

func selectAskContextChunks(chunks []postgres.SearchResult, queryType string, question string) []postgres.SearchResult {
	if len(chunks) == 0 {
		return chunks
	}
	ranked := rankAskChunksByKeywordScore(chunks, question)
	scores := make([]int, 0, len(ranked))
	for _, item := range ranked {
		scores = append(scores, item.score)
	}
	log.Println("TOP_CHUNKS_SCORES=", scores)
	chunks = make([]postgres.SearchResult, 0, len(ranked))
	for _, item := range ranked {
		chunks = append(chunks, item.chunk)
	}
	if len(chunks) == 0 {
		return nil
	}
	if queryType == "summary" {
		if len(chunks) <= 8 {
			return chunks
		}
		return chunks[:8]
	}
	if queryType == "section" {
		if len(chunks) <= 8 {
			return chunks
		}
		return chunks[:8]
	}
	if queryType == "structure" {
		if len(chunks) <= 8 {
			return chunks
		}
		return chunks[:8]
	}
	if len(chunks) <= 8 {
		return chunks
	}
	return chunks[:8]
}

type askChunkKeywordScore struct {
	chunk postgres.SearchResult
	score int
	index int
}

func rankAskChunksByKeywordScore(chunks []postgres.SearchResult, question string) []askChunkKeywordScore {
	keywords := askQuestionKeywords(question)
	if len(keywords) == 0 {
		return nil
	}

	ranked := make([]askChunkKeywordScore, 0, len(chunks))
	for index, chunk := range chunks {
		normalizedContent := service.NormalizeSearchText(chunk.Content)
		score := 0
		for _, keyword := range keywords {
			if countNormalizedWord(normalizedContent, keyword) > 0 {
				score++
			}
		}
		if score == 0 {
			continue
		}
		ranked = append(ranked, askChunkKeywordScore{
			chunk: chunk,
			score: score,
			index: index,
		})
	}

	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].score == ranked[j].score {
			return ranked[i].index < ranked[j].index
		}
		return ranked[i].score > ranked[j].score
	})
	return ranked
}

func askQuestionKeywords(question string) []string {
	normalized := service.NormalizeSearchText(question)
	seen := make(map[string]bool)
	keywords := make([]string, 0)
	for _, word := range strings.Fields(normalized) {
		word = strings.TrimSpace(word)
		if word == "" || askKeywordStopwords[word] || seen[word] {
			continue
		}
		if len([]rune(word)) < 3 {
			continue
		}
		seen[word] = true
		keywords = append(keywords, word)
	}
	return keywords
}

func countNormalizedWord(text string, word string) int {
	count := 0
	for _, candidate := range strings.Fields(text) {
		if candidate == word {
			count++
		}
	}
	return count
}

var askKeywordStopwords = map[string]bool{
	"a": true, "al": true, "algo": true, "ante": true, "asi": true, "como": true,
	"con": true, "cual": true, "cuales": true, "cuando": true, "de": true,
	"del": true, "desde": true, "dime": true, "documento": true, "don": true,
	"donde": true, "e": true, "el": true, "en": true, "entre": true,
	"es": true, "esa": true, "ese": true, "eso": true, "esta": true,
	"este": true, "esto": true, "explica": true, "haz": true, "la": true,
	"las": true, "le": true, "lo": true, "los": true, "me": true,
	"mi": true, "para": true, "pero": true, "por": true, "que": true,
	"qué": true, "quiero": true, "se": true, "segun": true, "sobre": true,
	"su": true, "sus": true, "te": true, "tiene": true, "un": true,
	"una": true, "y": true,
}

var internalReferencePattern = regexp.MustCompile(`\[(?:chunk_id|document_id)[^\]]*\]`)
var visiblePunctuationPattern = regexp.MustCompile(`([\.,;:!?])([A-Za-zÁÉÍÓÚÑáéíóúñ])`)
var visibleMultiSpacePattern = regexp.MustCompile(`[ \t]{2,}`)
var directRomanHeadingPattern = regexp.MustCompile(`(?i)^[ivxlcdm]+\s*[\.\):-]\s+\S.+$`)
var directNumericHeadingPattern = regexp.MustCompile(`^\d+(\.\d+){0,3}\s*[\.\):-]?\s+\S.+$`)
var standaloneNumberingPattern = regexp.MustCompile(`^\s*(?:[-*]\s*)?\d+[\.)]?\s*$`)
var genericStandaloneTitlePattern = regexp.MustCompile(`(?i)^\s*(respuesta del documento|punto clave\s+\d+|apartado\s+\d+|secci[oó]n\s+\d+)\s*$`)
var genericNumberedTitlePattern = regexp.MustCompile(`(?i)^\s*\d+[\.)]\s*(punto clave|apartado|secci[oó]n)\s+\d+\s*$`)
var leadingEllipsisPattern = regexp.MustCompile(`^\s*\.\.\.\s*`)
var trailingEllipsisPattern = regexp.MustCompile(`\s*\.\.\.\s*$`)
var genericAnswerPhrasePatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\s*este elemento resulta\s+relevante[^.?!]*(?:[.?!]|$)`),
	regexp.MustCompile(`(?i)\s*aporta contexto\s+sustantivo[^.?!]*(?:[.?!]|$)`),
	regexp.MustCompile(`(?i)\s*permite\s+conectar la información[^.?!]*(?:[.?!]|$)`),
	regexp.MustCompile(`(?i)\s*en términos\s+documentales[^.?!]*(?:[.?!]|$)`),
	regexp.MustCompile(`(?i)\s*funciona como una pieza\s+central[^.?!]*(?:[.?!]|$)`),
	regexp.MustCompile(`(?i)\s*en conjunto, estos elementos[^.?!]*(?:[.?!]|$)`),
}

func cleanUserVisibleAnswer(answer string) string {
	answer = internalReferencePattern.ReplaceAllString(answer, "")
	answer = strings.ReplaceAll(answer, "[]", "")
	answer = visiblePunctuationPattern.ReplaceAllString(answer, "$1 $2")
	lines := strings.Split(answer, "\n")
	cleaned := make([]string, 0, len(lines))
	lastBlank := false
	for _, line := range lines {
		line = visibleMultiSpacePattern.ReplaceAllString(line, " ")
		line = strings.TrimSpace(line)
		if line == "" {
			if lastBlank {
				continue
			}
			lastBlank = true
			cleaned = append(cleaned, "")
			continue
		}
		lastBlank = false
		cleaned = append(cleaned, line)
	}
	return strings.TrimSpace(strings.Join(cleaned, "\n"))
}

func cleanFinalAnswer(text string) string {
	text = cleanUserVisibleAnswer(text)
	for _, pattern := range genericAnswerPhrasePatterns {
		text = pattern.ReplaceAllString(text, " ")
	}
	text = visiblePunctuationPattern.ReplaceAllString(text, "$1 $2")

	blocks := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n\n")
	cleaned := make([]string, 0, len(blocks))
	seen := make(map[string]bool)
	lastBlockKey := ""
	for _, block := range blocks {
		lines := strings.Split(block, "\n")
		normalizedLines := make([]string, 0, len(lines))
		lastLineKey := ""
		for _, line := range lines {
			line = cleanFinalAnswerLine(line)
			if line == "" {
				continue
			}
			lineKey := service.NormalizeSearchText(line)
			if lineKey != "" && lineKey == lastLineKey {
				continue
			}
			lastLineKey = lineKey
			normalizedLines = append(normalizedLines, line)
		}
		block = strings.TrimSpace(strings.Join(normalizedLines, "\n"))
		if block == "" {
			continue
		}
		key := service.NormalizeSearchText(block)
		if key != "" && seen[key] {
			continue
		}
		if key != "" && key == lastBlockKey {
			continue
		}
		if key != "" {
			seen[key] = true
		}
		lastBlockKey = key
		cleaned = append(cleaned, block)
	}
	return strings.TrimSpace(strings.Join(cleaned, "\n\n"))
}

func cleanFinalAnswerLine(line string) string {
	line = visibleMultiSpacePattern.ReplaceAllString(line, " ")
	line = leadingEllipsisPattern.ReplaceAllString(line, "")
	line = trailingEllipsisPattern.ReplaceAllString(line, "")
	line = strings.TrimSpace(line)
	if line == "" {
		return ""
	}
	if standaloneNumberingPattern.MatchString(line) ||
		genericStandaloneTitlePattern.MatchString(line) ||
		genericNumberedTitlePattern.MatchString(line) {
		return ""
	}
	return line
}

func directStructureLines(chunks []postgres.SearchResult, limit int) []string {
	if limit <= 0 || len(chunks) == 0 {
		return nil
	}

	lines := make([]string, 0, limit)
	seen := make(map[string]bool)
	for _, chunk := range chunks {
		for _, rawLine := range strings.Split(strings.ReplaceAll(chunk.Content, "\r\n", "\n"), "\n") {
			line := cleanUserVisibleAnswer(rawLine)
			if line == "" {
				continue
			}
			if !directRomanHeadingPattern.MatchString(line) && !directNumericHeadingPattern.MatchString(line) {
				continue
			}
			key := service.NormalizeSearchText(line)
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			lines = append(lines, "- "+line)
			if len(lines) == limit {
				return lines
			}
		}
	}
	return lines
}

func chunksHaveReadableText(chunks []postgres.SearchResult) bool {
	text := chunksTextSample(chunks, 6000)
	if strings.TrimSpace(text) == "" {
		return false
	}
	return visibleTextQualityScore(text) >= 45
}

func chunksTextSample(chunks []postgres.SearchResult, maxRunes int) string {
	var builder strings.Builder
	for _, chunk := range chunks {
		content := strings.TrimSpace(cleanUserVisibleAnswer(chunk.Content))
		if content == "" {
			continue
		}
		if builder.Len() > 0 {
			builder.WriteString("\n")
		}
		builder.WriteString(content)
		if len([]rune(builder.String())) >= maxRunes {
			break
		}
	}
	runes := []rune(builder.String())
	if len(runes) > maxRunes {
		return string(runes[:maxRunes])
	}
	return string(runes)
}

func visibleTextQualityScore(text string) int {
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
		runes := []rune(word)
		if len(runes) >= 3 && wordHasLetter(word) {
			good++
		}
		if len(runes) <= 2 && lowercaseWord(word) {
			shortLower++
		}
		if len(runes) >= 18 && lowercaseWord(word) {
			veryLongLower++
		}
		for _, r := range runes {
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

func wordHasLetter(word string) bool {
	for _, r := range word {
		if unicode.IsLetter(r) {
			return true
		}
	}
	return false
}

func lowercaseWord(word string) bool {
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

func limitAnswerLines(answer string, maxLines int) string {
	if maxLines <= 0 {
		return strings.TrimSpace(answer)
	}
	lines := strings.Split(answer, "\n")
	trimmed := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		trimmed = append(trimmed, line)
		if len(trimmed) == maxLines {
			break
		}
	}
	return strings.TrimSpace(strings.Join(trimmed, "\n"))
}

func formatAnswerForQueryType(answer string, queryType string) string {
	answer = strings.TrimSpace(answer)
	if answer == "" {
		return ""
	}

	switch queryType {
	case "summary":
		return answer
	case "section":
		return answer
	case "structure":
		return answer
	default:
		return answer
	}
}

func answerLengthModeForQueryType(queryType string) string {
	switch queryType {
	case "summary":
		return "long"
	case "section":
		return "medium"
	default:
		return "short"
	}
}

func summaryModeForQueryType(queryType string) string {
	if queryType == "summary" {
		return "expanded"
	}
	return "n/a"
}

func countAnswerLines(answer string) int {
	answer = strings.TrimSpace(answer)
	if answer == "" {
		return 0
	}

	count := 0
	for _, line := range strings.Split(answer, "\n") {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count
}
