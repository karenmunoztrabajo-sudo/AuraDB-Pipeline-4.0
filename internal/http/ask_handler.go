package http

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"regexp"
	"strings"
	"unicode"

	"auradb-pipeline/internal/repository/postgres"
	"auradb-pipeline/internal/service"
)

type AskHandler struct {
	searchService *service.SearchService
	searchRepo    *postgres.SearchRepository
	chatService   *service.OllamaChatService
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
	askTopK          = 2
	askSummaryTopK   = 25
	askSectionTopK   = 4
	askStructureTopK = 6
)

func NewAskHandler(searchService *service.SearchService, searchRepo *postgres.SearchRepository, chatService *service.OllamaChatService, askLogRepo *postgres.AskLogRepository) *AskHandler {
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
	documentID := r.URL.Query().Get("document_id")
	documentIDs := parseDocumentIDs(r)
	if len(documentIDs) == 0 && strings.TrimSpace(documentID) != "" {
		documentIDs = []string{strings.TrimSpace(documentID)}
	}
	if strings.TrimSpace(documentID) == "" && len(documentIDs) > 0 {
		documentID = strings.TrimSpace(documentIDs[0])
	}
	selectedDocumentIDs := strings.Join(documentIDs, ",")
	multiDocumentMode := len(documentIDs) > 1

	if queryType == "summary" {
		log.Printf("summary_document_id=%s", documentID)
		chunks, err := h.searchService.GetAllChunksByDocumentID(
			r.Context(),
			documentID,
		)
		log.Printf("summary_chunks_found=%d", len(chunks))
		log.Printf("chunks_found=%d", len(chunks))
		log.Printf("summary_direct_mode=true summary_document_id=%s summary_chunks_found=%d", documentID, len(chunks))
		if err != nil || len(chunks) == 0 {
			log.Printf("summary_no_chunks_found=true summary_document_id=%s", documentID)
			answer := "El documento aún no tiene contenido procesado."
			writeAskResponse(w, question, answer, "", nil, nil)
			return
		}

		if len(chunks) > askSummaryTopK {
			chunks = chunks[:askSummaryTopK]
		}

		log.Printf("summary_direct_mode=true summary_document_id=%s summary_chunks_used=%d", documentID, len(chunks))

		contextText, sources := buildAskContext(chunks)
		if !chunksHaveReadableText(chunks) {
			answer := "El documento fue procesado, pero el texto extraído no tiene calidad suficiente para generar un resumen confiable."
			writeAskResponse(w, question, answer, contextText, chunks, sources)
			return
		}

		log.Printf("ask_answer_started=true")
		summaryOllamaFailed := false
		summaryLocalFallbackUsed := false

		answer, err := h.chatService.Answer(r.Context(), question, contextText, "summary")
		if err != nil {
			summaryOllamaFailed = true
			summaryLocalFallbackUsed = true
			log.Printf("ask_error_recovered=true")
			log.Printf("local_fallback_used=true")
			answer = buildLocalSummaryFallback(chunks)
		}

		log.Printf("ask_answer_finished=true")
		log.Printf("summary_ollama_failed=%t", summaryOllamaFailed)
		log.Printf("summary_local_fallback_used=%t", summaryLocalFallbackUsed)

		answer = cleanUserVisibleAnswer(answer)

		writeAskResponse(w, question, answer, contextText, chunks, sources)
		return
	}

	if queryType == "structure" && strings.TrimSpace(documentID) != "" {
		chunks, err := h.searchService.GetAllChunksByDocumentID(r.Context(), documentID)
		log.Printf("structure_direct_mode=true document_id=%s chunks_found=%d", documentID, len(chunks))
		log.Printf("chunks_found=%d", len(chunks))
		if err == nil && len(chunks) > 0 {
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
		log.Printf("ask_error_recovered=true")
		log.Printf("local_fallback_used=true")
		writeAskResponse(w, question, cleanUserVisibleAnswer("No pude recuperar contexto del documento en este momento, pero puedes intentar una pregunta más específica."), "", nil, nil)
		return
	}

	mainEntity := service.MainEntityForQuery(normalizedQuestionForRetrieval)
	reasonEntityRejected := service.MainEntityRejectedReasonForQuery(normalizedQuestionForRetrieval)
	entityFilterApplied := service.ShouldApplyMainEntityFilter(normalizedQuestionForRetrieval)
	entityExtractionMode := service.EntityExtractionModeForQuery(normalizedQuestionForRetrieval)

	prioritizedChunks, restoredChunksAfterEntityFilter, askEntityRefilterRemovedAll := prioritizeAskChunksByEntity(retrievedChunks, mainEntity, entityFilterApplied)
	contextChunks := selectAskContextChunks(prioritizedChunks, queryType)
	summaryDirectChunkMode := queryType == "summary" && len(documentIDs) > 0

	log.Printf(
		"ask_retrieval query=%q normalized_question_for_retrieval=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t retrieval_chunks_count=%d main_entity_detected=%q entity_filter_applied=%t entity_extraction_mode=%q reason_entity_rejected=%q restored_chunks_after_entity_filter=%t ask_entity_refilter_removed_all=%t section_query_detected=%t section_term=%q selected_chunks=%s summary_direct_chunk_mode=%t summary_chunks_used=%d",
		question,
		normalizedQuestionForRetrieval,
		service.NormalizeSearchText(normalizedQuestionForRetrieval),
		queryType,
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
		answer := "No encontré información suficiente en el documento para responder esa pregunta."
		if queryType == "summary" {
			answer = "El documento aún no ha terminado de procesarse o no tiene texto extraíble."
		}
		contextText := ""
		sources := []askSource{}
		answerLengthMode := answerLengthModeForQueryType(queryType)
		finalAnswerLineCount := countAnswerLines(answer)

		log.Printf(
			"ask_answer query=%q normalized_question_for_retrieval=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t retrieval_chunks_count=%d extractive_answer_used=%t extractive_answer_type=%q model_fallback_used=%t restored_chunks_after_entity_filter=%t answer_cleanup_applied=%t answer_length_mode=%s final_answer_line_count=%d summary_direct_chunk_mode=%t summary_chunks_used=%d ask_answer_started=%t ask_answer_finished=%t",
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
		writeAskResponse(w, question, answer, contextText, []postgres.SearchResult{}, sources)
		return
	}

	contextText, sources := buildAskContext(contextChunks)

	answer := ""
	extractiveAnswerUsed := false
	extractiveAnswerType := "none"
	modelFallbackUsed := false
	answerCleanupApplied := false
	localFallbackUsed := false
	summaryChunksUsed := 0
	summarySectionsDetected := 0
	modelTokensLimit := 0

	if queryType == "question" {
		if extractedAnswer, extractedType, cleanupApplied, ok := service.TryExtractAnswer(normalizedQuestionForRetrieval, contextChunks); ok {
			answer = extractedAnswer
			extractiveAnswerUsed = true
			extractiveAnswerType = extractedType
			answerCleanupApplied = cleanupApplied
		}
	}

	if !extractiveAnswerUsed {
		modelFallbackUsed = true
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
					log.Printf("ask_summary_section_error title=%q error=%v", group.Title, sectionErr)
					sectionSummary = strings.TrimSpace(sectionContext)
				}
				sectionSummary = cleanUserVisibleAnswer(sectionSummary)
				if sectionSummary == "" {
					sectionSummary = "No encontré esa información en el documento."
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
			log.Println("ERROR /ask ->", err.Error())
			localFallbackUsed = true
			log.Printf("ask_error_recovered=true")
			log.Printf("local_fallback_used=true")
			answer = buildLocalAnswerFallback(question, contextChunks, queryType)
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
		localFallbackUsed = true
		answer = buildLocalAnswerFallback(question, contextChunks, queryType)
		answer = cleanUserVisibleAnswer(answer)
		answer = formatAnswerForQueryType(answer, queryType)
	}
	answerLengthMode := answerLengthModeForQueryType(queryType)
	finalAnswerLineCount := countAnswerLines(answer)

	log.Printf(
		"ask_answer query=%q normalized_question_for_retrieval=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t retrieval_chunks_count=%d extractive_answer_used=%t extractive_answer_type=%q model_fallback_used=%t restored_chunks_after_entity_filter=%t answer_cleanup_applied=%t answer_length_mode=%s final_answer_line_count=%d summary_mode=%s summary_sections_detected=%d summary_chunks_used=%d model_tokens_limit=%d answer_length_lines=%d summary_direct_chunk_mode=%t ask_answer_started=%t ask_answer_finished=%t local_fallback_used=%t",
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
		modelFallbackUsed,
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
		localFallbackUsed,
	)

	h.persistAskLog(r, ipcCtx.TenantID, ipcCtx.UserID, firstDocumentID(documentIDs, documentID), question, answer, contextText, sources)
	writeAskResponse(w, question, answer, contextText, contextChunks, sources)
}

func (h *AskHandler) answerSummary(w http.ResponseWriter, r *http.Request, tenantID string, userID string, question string, normalizedQuestionForRetrieval string, documentID string, documentIDs []string, selectedDocumentIDs string, multiDocumentMode bool) {
	contextChunks, err := h.loadSummaryChunks(r.Context(), tenantID, documentIDs)
	if err != nil {
		http.Error(w, "error en búsqueda: "+err.Error(), http.StatusInternalServerError)
		return
	}

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

	if len(contextChunks) == 0 {
		answer := "El documento aún no ha sido procesado o no contiene texto legible."
		contextText := ""
		sources := []askSource{}

		log.Printf(
			"ask_answer query=%q normalized_question_for_retrieval=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t retrieval_chunks_count=%d extractive_answer_used=%t extractive_answer_type=%q model_fallback_used=%t restored_chunks_after_entity_filter=%t answer_cleanup_applied=%t answer_length_mode=%s final_answer_line_count=%d summary_mode=%s summary_sections_detected=%d summary_chunks_used=%d model_tokens_limit=%d answer_length_lines=%d summary_direct_mode=true ask_answer_started=%t ask_answer_finished=%t",
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
		log.Println("ERROR /ask ->", err.Error())
		answer = "No encontré una respuesta suficientemente clara en el documento."
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
		answer = "No encontré una respuesta suficientemente clara en el documento."
	}

	finalAnswerLineCount := countAnswerLines(answer)
	modelTokensLimit := service.ModelTokensLimitForQueryType("summary")

	log.Printf(
		"ask_answer query=%q normalized_question_for_retrieval=%q normalized_query=%q query_type=%q document_id=%s selected_document_ids=%q multi_document_mode=%t retrieval_chunks_count=%d extractive_answer_used=%t extractive_answer_type=%q model_fallback_used=%t restored_chunks_after_entity_filter=%t answer_cleanup_applied=%t answer_length_mode=%s final_answer_line_count=%d summary_mode=%s summary_sections_detected=%d summary_chunks_used=%d model_tokens_limit=%d answer_length_lines=%d summary_direct_mode=true ask_answer_started=%t ask_answer_finished=%t",
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
		false,
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
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(askResponse{
		Question: question,
		Answer:   answer,
		Context:  contextText,
		Chunks:   chunks,
		Sources:  sources,
	})
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

func buildLocalSummaryFallback(chunks []postgres.SearchResult) string {
	if !chunksHaveReadableText(chunks) {
		return "El documento fue procesado, pero el texto extraído no tiene calidad suficiente para generar un resumen confiable."
	}

	points := buildSummaryFallbackPoints(chunks, 8)

	if len(points) == 0 {
		return "El documento fue procesado, pero el texto extraído no tiene calidad suficiente para generar un resumen confiable."
	}

	return "No pude generar el resumen con IA, pero encontré estos puntos principales del documento:\n\n" + strings.Join(points, "\n")
}

func buildLocalAnswerFallback(question string, chunks []postgres.SearchResult, queryType string) string {
	if queryType == "summary" {
		return buildLocalSummaryFallback(chunks)
	}

	points := make([]string, 0, 6)
	for _, chunk := range chunks {
		text := strings.TrimSpace(cleanUserVisibleAnswer(chunk.Content))
		if text == "" {
			continue
		}
		text = collapseWhitespace(text)
		text = firstSentenceOrExcerpt(text, 180)
		if text == "" {
			continue
		}
		points = append(points, "- "+text)
		if len(points) == 6 {
			break
		}
	}

	if len(points) == 0 {
		return "No encontré una respuesta suficientemente clara en el documento."
	}

	intro := "No pude responder con IA, pero encontré estos fragmentos relevantes del documento:"
	if queryType == "structure" {
		intro = "No pude extraer la estructura con IA, pero encontré estos elementos relevantes:"
	}
	if queryType == "section" {
		intro = "No pude responder con IA, pero encontré estos puntos de la sección solicitada:"
	}
	_ = question
	return intro + "\n\n" + strings.Join(points, "\n")
}

func collapseWhitespace(text string) string {
	return strings.Join(strings.Fields(text), " ")
}

func buildSummaryFallbackPoints(chunks []postgres.SearchResult, limit int) []string {
	if limit <= 0 || len(chunks) == 0 {
		return nil
	}

	selectedChunkIndexes := distributedChunkIndexes(len(chunks), limit*2)
	seen := make(map[string]bool)
	points := make([]string, 0, limit)

	for _, idx := range selectedChunkIndexes {
		if idx < 0 || idx >= len(chunks) {
			continue
		}
		sentences := extractCleanSummarySentences(chunks[idx].Content)
		for _, sentence := range sentences {
			key := strings.ToLower(sentence)
			if seen[key] {
				continue
			}
			seen[key] = true
			points = append(points, "- "+sentence)
			if len(points) == limit {
				return points
			}
		}
	}

	for _, chunk := range chunks {
		sentences := extractCleanSummarySentences(chunk.Content)
		for _, sentence := range sentences {
			key := strings.ToLower(sentence)
			if seen[key] {
				continue
			}
			seen[key] = true
			points = append(points, "- "+sentence)
			if len(points) == limit {
				return points
			}
		}
	}

	return points
}

func distributedChunkIndexes(total int, target int) []int {
	if total <= 0 || target <= 0 {
		return nil
	}
	if total <= target {
		indexes := make([]int, total)
		for i := 0; i < total; i++ {
			indexes[i] = i
		}
		return indexes
	}

	indexes := make([]int, 0, target)
	seen := make(map[int]bool)
	for i := 0; i < target; i++ {
		idx := int(float64(i) * float64(total-1) / float64(target-1))
		if !seen[idx] {
			seen[idx] = true
			indexes = append(indexes, idx)
		}
	}
	return indexes
}

func extractCleanSummarySentences(text string) []string {
	text = strings.TrimSpace(cleanUserVisibleAnswer(text))
	if text == "" {
		return nil
	}
	text = normalizeFallbackSummaryText(text)
	if text == "" {
		return nil
	}

	rawParts := strings.FieldsFunc(text, func(r rune) bool {
		return r == '.' || r == ';' || r == '\n' || r == '!' || r == '?'
	})
	sentences := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		sentence := cleanSummarySentence(part)
		if !isUsefulSummarySentence(sentence) {
			continue
		}
		sentences = append(sentences, sentence)
	}
	return sentences
}

func normalizeFallbackSummaryText(text string) string {
	text = collapseWhitespace(text)
	text = strings.ReplaceAll(text, " ,", ",")
	text = strings.ReplaceAll(text, " .", ".")
	text = strings.ReplaceAll(text, " ;", ";")
	return strings.TrimSpace(text)
}

func fixFallbackMergedWords(text string) string {
	return collapseWhitespace(text)
}

func cleanSummarySentence(text string) string {
	text = strings.TrimSpace(text)
	text = strings.Trim(text, "-•* \t")
	text = normalizeFallbackSummaryText(text)
	if text == "" {
		return ""
	}
	runes := []rune(text)
	if len(runes) > 220 {
		text = strings.TrimSpace(string(runes[:220])) + "..."
	}
	return text
}

func isUsefulSummarySentence(text string) bool {
	if text == "" {
		return false
	}
	words := strings.Fields(text)
	if len(words) < 8 || len(words) > 35 {
		return false
	}
	normalized := service.NormalizeSearchText(text)
	if normalized == "" {
		return false
	}
	for _, noise := range []string{"isbn", "bibliografia", "referencias", "works cited", "fuentes", "copyright", "editorial"} {
		if strings.Contains(normalized, noise) {
			return false
		}
	}
	if strings.Count(text, "=") > 2 || strings.Count(text, "|") > 2 {
		return false
	}
	letters := 0
	suspiciousWords := 0
	for _, r := range text {
		if ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') || ('á' <= r && r <= 'ú') || ('Á' <= r && r <= 'Ú') || r == 'ñ' || r == 'Ñ' {
			letters++
		}
	}
	for _, word := range words {
		trimmed := strings.Trim(word, ".,;:!?()[]{}\"'")
		if trimmed == "" {
			continue
		}
		runes := []rune(trimmed)
		if len(runes) >= 16 && !strings.ContainsAny(trimmed, "ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÚÑ") {
			suspiciousWords++
			continue
		}
		if len(runes) <= 2 {
			suspiciousWords++
		}
	}
	if suspiciousWords > len(words)/3 {
		return false
	}
	return letters >= len([]rune(text))/2
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

func selectAskContextChunks(chunks []postgres.SearchResult, queryType string) []postgres.SearchResult {
	if len(chunks) == 0 {
		return chunks
	}
	if queryType == "summary" {
		if len(chunks) <= askSummaryTopK {
			return chunks
		}
		return chunks[:askSummaryTopK]
	}
	if queryType == "section" {
		if len(chunks) <= askSectionTopK {
			return chunks
		}
		return chunks[:askSectionTopK]
	}
	if queryType == "structure" {
		if len(chunks) <= askStructureTopK {
			return chunks
		}
		return chunks[:askStructureTopK]
	}
	if len(chunks) == 1 {
		return chunks[:1]
	}
	return chunks[:2]
}

var internalReferencePattern = regexp.MustCompile(`\[(?:chunk_id|document_id)[^\]]*\]`)
var visiblePunctuationPattern = regexp.MustCompile(`([\.,;:!?])([A-Za-zÁÉÍÓÚÑáéíóúñ])`)
var visibleMultiSpacePattern = regexp.MustCompile(`[ \t]{2,}`)
var directRomanHeadingPattern = regexp.MustCompile(`(?i)^[ivxlcdm]+\s*[\.\):-]\s+\S.+$`)
var directNumericHeadingPattern = regexp.MustCompile(`^\d+(\.\d+){0,3}\s*[\.\):-]?\s+\S.+$`)

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
		return limitAnswerLines(answer, 6)
	case "structure":
		return limitAnswerLines(answer, 8)
	default:
		return limitAnswerLines(answer, 4)
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
