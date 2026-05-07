package postgres

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type SearchResult struct {
	ChunkID      string    `json:"chunk_id"`
	DocumentID   string    `json:"document_id"`
	SectionTitle string    `json:"section_title,omitempty"`
	Content      string    `json:"content"`
	ChunkIndex   int       `json:"chunk_index"`
	Embedding    []float64 `json:"-"`
	Score        float64   `json:"score"`
}

type SearchRepository struct {
	db *pgxpool.Pool
}

type GlobalSearchDiagnostics struct {
	TotalDocuments int
	TotalChunks    int
	Documents      []GlobalDocumentSample
	Chunks         []GlobalChunkSample
}

type GlobalDocumentSample struct {
	DocumentID string
	Name       string
	TenantID   string
}

type GlobalChunkSample struct {
	DocumentID string
	Preview    string
}

func NewSearchRepository(db *pgxpool.Pool) *SearchRepository {
	return &SearchRepository{db: db}
}

func (r *SearchRepository) GetDocumentFilename(ctx context.Context, tenantID string, documentID string) (string, error) {
	if strings.TrimSpace(documentID) == "" {
		return "", nil
	}

	var filename string
	err := r.db.QueryRow(ctx, `
		SELECT COALESCE(NULLIF(original_name, ''), NULLIF(logical_name, ''), NULLIF(stored_name, ''), '')
		FROM documents
		WHERE tenant_id = $1 AND id = $2
	`, tenantID, documentID).Scan(&filename)
	if err != nil {
		return "", err
	}
	return filename, nil
}

func (r *SearchRepository) GetDocumentIDsByCollectionID(ctx context.Context, tenantID string, collectionID string) ([]string, error) {
	if strings.TrimSpace(collectionID) == "" {
		return nil, nil
	}

	rows, err := r.db.Query(ctx, `
		SELECT d.id
		FROM documents d
		INNER JOIN collections c ON c.id = d.collection_id AND c.tenant_id = d.tenant_id
		WHERE d.tenant_id = $1 AND c.id = $2
		ORDER BY COALESCE(d.uploaded_at, d.created_at, NOW()) DESC, d.id ASC
	`, tenantID, collectionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	documentIDs := make([]string, 0)
	for rows.Next() {
		var documentID string
		if err := rows.Scan(&documentID); err != nil {
			return nil, err
		}
		documentIDs = append(documentIDs, documentID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return documentIDs, nil
}

func (r *SearchRepository) GetFavoriteDocumentIDs(ctx context.Context, tenantID string) ([]string, error) {
	rows, err := r.db.Query(ctx, `
		SELECT id
		FROM documents
		WHERE tenant_id = $1 AND is_favorite = true
		ORDER BY COALESCE(uploaded_at, created_at, NOW()) DESC, id ASC
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	documentIDs := make([]string, 0)
	for rows.Next() {
		var documentID string
		if err := rows.Scan(&documentID); err != nil {
			return nil, err
		}
		documentIDs = append(documentIDs, documentID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return documentIDs, nil
}

func (r *SearchRepository) GetDocumentIDsByTag(ctx context.Context, tenantID string, tag string) ([]string, error) {
	tag = strings.TrimSpace(strings.ToLower(tag))
	tag = strings.TrimPrefix(tag, "#")
	if tag == "" {
		return nil, nil
	}

	rows, err := r.db.Query(ctx, `
		SELECT d.id
		FROM document_tags dt
		INNER JOIN documents d ON d.id = dt.document_id
		WHERE d.tenant_id = $1 AND dt.tag = $2
		ORDER BY COALESCE(d.uploaded_at, d.created_at, NOW()) DESC, d.id ASC
	`, tenantID, tag)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	documentIDs := make([]string, 0)
	for rows.Next() {
		var documentID string
		if err := rows.Scan(&documentID); err != nil {
			return nil, err
		}
		documentIDs = append(documentIDs, documentID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return documentIDs, nil
}

func (r *SearchRepository) GetCollectionName(ctx context.Context, tenantID string, collectionID string) (string, error) {
	if strings.TrimSpace(collectionID) == "" {
		return "", nil
	}

	var name string
	err := r.db.QueryRow(ctx, `
		SELECT name
		FROM collections
		WHERE tenant_id = $1 AND id = $2
	`, tenantID, collectionID).Scan(&name)
	if err != nil {
		return "", err
	}
	return name, nil
}

func (r *SearchRepository) GetChunksWithEmbeddings(ctx context.Context, tenantID string) ([]SearchResult, error) {
	return r.GetChunksWithEmbeddingsByDocumentIDs(ctx, tenantID, nil)
}

func (r *SearchRepository) GetChunksByDocumentID(ctx context.Context, documentID string) ([]SearchResult, error) {
	if strings.TrimSpace(documentID) == "" {
		return nil, nil
	}

	rows, err := r.db.Query(ctx, `
		SELECT
			id AS chunk_id,
			document_id,
			chunk_index,
			content,
			1.0 AS score
		FROM chunks
		WHERE document_id = $1
		ORDER BY chunk_index ASC
	`, documentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]SearchResult, 0)
	for rows.Next() {
		var item SearchResult
		if err := rows.Scan(
			&item.ChunkID,
			&item.DocumentID,
			&item.ChunkIndex,
			&item.Content,
			&item.Score,
		); err != nil {
			return nil, err
		}
		results = append(results, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (r *SearchRepository) GetChunksByDocumentIDs(ctx context.Context, tenantID string, documentIDs []string) ([]SearchResult, error) {
	query := `
		SELECT
			c.id,
			c.document_id,
			COALESCE(c.section_title, ''),
			c.content,
			c.chunk_index
		FROM chunks c
		WHERE c.tenant_id = $1
	`
	args := []any{tenantID}
	if len(documentIDs) > 0 {
		args = append(args, documentIDs)
		query += " AND c.document_id = ANY($2)"
	}
	query += " ORDER BY c.created_at DESC, c.document_id ASC, c.chunk_index ASC"

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SearchResult

	for rows.Next() {
		var item SearchResult

		err := rows.Scan(
			&item.ChunkID,
			&item.DocumentID,
			&item.SectionTitle,
			&item.Content,
			&item.ChunkIndex,
		)
		if err != nil {
			return nil, err
		}

		results = append(results, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (r *SearchRepository) GetChunksWithEmbeddingsByDocumentID(ctx context.Context, tenantID string, documentID string) ([]SearchResult, error) {
	if strings.TrimSpace(documentID) == "" {
		return nil, nil
	}
	return r.GetChunksWithEmbeddingsByDocumentIDs(ctx, tenantID, []string{documentID})
}

func (r *SearchRepository) GetChunksWithEmbeddingsByDocumentIDs(ctx context.Context, tenantID string, documentIDs []string) ([]SearchResult, error) {
	query := `
		SELECT
			c.id,
			c.document_id,
			COALESCE(c.section_title, ''),
			c.content,
			c.chunk_index,
			e.embedding
		FROM chunks c
		INNER JOIN embeddings e ON e.chunk_id = c.id
		WHERE c.tenant_id = $1
	`
	args := []any{tenantID}
	if len(documentIDs) > 0 {
		args = append(args, documentIDs)
		query += " AND c.document_id = ANY($2)"
	}
	query += " ORDER BY c.created_at DESC"

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SearchResult

	for rows.Next() {
		var item SearchResult
		var rawEmbedding []byte

		err := rows.Scan(
			&item.ChunkID,
			&item.DocumentID,
			&item.SectionTitle,
			&item.Content,
			&item.ChunkIndex,
			&rawEmbedding,
		)
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal(rawEmbedding, &item.Embedding); err != nil {
			return nil, err
		}

		results = append(results, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (r *SearchRepository) GetChunksByText(ctx context.Context, tenantID string, documentID string, searchText string) ([]SearchResult, error) {
	if strings.TrimSpace(documentID) == "" {
		return nil, nil
	}
	return r.GetChunksByTextByDocumentIDs(ctx, tenantID, []string{documentID}, searchText)
}

func (r *SearchRepository) GetChunksByTextByDocumentIDs(ctx context.Context, tenantID string, documentIDs []string, searchText string) ([]SearchResult, error) {
	query := `
		SELECT
			c.id,
			c.document_id,
			COALESCE(c.section_title, ''),
			c.content,
			c.chunk_index
		FROM chunks c
		WHERE c.tenant_id = $1
	`
	args := []any{tenantID}
	nextArg := 2

	if len(documentIDs) > 0 {
		query += " AND c.document_id = ANY($" + strconv.Itoa(nextArg) + ")"
		args = append(args, documentIDs)
		nextArg++
	}

	terms := searchTerms(searchText)
	if len(terms) > 0 {
		query += " AND ("
		for i, term := range terms {
			if i > 0 {
				query += " OR "
			}
			query += "c.content ILIKE $" + strconv.Itoa(nextArg)
			args = append(args, "%"+term+"%")
			nextArg++
		}
		query += ")"
	}

	query += " ORDER BY c.created_at DESC, c.document_id ASC, c.chunk_index ASC"

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SearchResult

	for rows.Next() {
		var item SearchResult

		err := rows.Scan(
			&item.ChunkID,
			&item.DocumentID,
			&item.SectionTitle,
			&item.Content,
			&item.ChunkIndex,
		)
		if err != nil {
			return nil, err
		}

		results = append(results, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (r *SearchRepository) SearchGlobalChunksByText(ctx context.Context, tenantID string, searchText string, limit int) ([]SearchResult, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}

	terms := globalSearchTerms(searchText)
	if len(terms) == 0 {
		return nil, nil
	}

	args := []any{}
	nextArg := 1
	if strings.TrimSpace(tenantID) != "" {
		args = append(args, tenantID)
		nextArg++
	}
	termArgStart := nextArg
	for _, term := range terms {
		args = append(args, "%"+term+"%")
		nextArg++
	}

	query := `
		SELECT
			c.id,
			c.document_id,
			COALESCE(c.section_title, ''),
			c.content,
			c.chunk_index,
			(
	`
	for i := range terms {
		if i > 0 {
			query += " + "
		}
		query += "CASE WHEN c.content ILIKE $" + strconv.Itoa(termArgStart+i) + " THEN 1 ELSE 0 END"
	}
	query += `
			)::float AS score
		FROM chunks c
		INNER JOIN documents d ON d.id = c.document_id
		WHERE
	`
	if strings.TrimSpace(tenantID) != "" {
		query += " d.tenant_id = $1 AND c.tenant_id = $1 AND "
	}
	query += `(
	`
	for i := range terms {
		if i > 0 {
			query += " OR "
		}
		query += "c.content ILIKE $" + strconv.Itoa(termArgStart+i)
	}
	query += `
		  )
		ORDER BY score DESC, c.created_at DESC, c.document_id ASC, c.chunk_index ASC
		LIMIT $` + strconv.Itoa(nextArg)
	args = append(args, limit)

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]SearchResult, 0)
	for rows.Next() {
		var item SearchResult
		if err := rows.Scan(
			&item.ChunkID,
			&item.DocumentID,
			&item.SectionTitle,
			&item.Content,
			&item.ChunkIndex,
			&item.Score,
		); err != nil {
			return nil, err
		}
		results = append(results, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (r *SearchRepository) GlobalSearchTerms(searchText string) []string {
	return globalSearchTerms(searchText)
}

func (r *SearchRepository) GetGlobalSearchDiagnostics(ctx context.Context) (GlobalSearchDiagnostics, error) {
	var diagnostics GlobalSearchDiagnostics
	if err := r.db.QueryRow(ctx, `SELECT COUNT(*) FROM documents`).Scan(&diagnostics.TotalDocuments); err != nil {
		return diagnostics, err
	}
	if err := r.db.QueryRow(ctx, `SELECT COUNT(*) FROM chunks`).Scan(&diagnostics.TotalChunks); err != nil {
		return diagnostics, err
	}

	documentRows, err := r.db.Query(ctx, `
		SELECT
			id,
			COALESCE(NULLIF(original_name, ''), NULLIF(logical_name, ''), NULLIF(stored_name, ''), ''),
			tenant_id
		FROM documents
		ORDER BY COALESCE(uploaded_at, created_at, NOW()) DESC, id ASC
		LIMIT 5
	`)
	if err != nil {
		return diagnostics, err
	}
	defer documentRows.Close()

	for documentRows.Next() {
		var item GlobalDocumentSample
		if err := documentRows.Scan(&item.DocumentID, &item.Name, &item.TenantID); err != nil {
			return diagnostics, err
		}
		diagnostics.Documents = append(diagnostics.Documents, item)
	}
	if err := documentRows.Err(); err != nil {
		return diagnostics, err
	}

	chunkRows, err := r.db.Query(ctx, `
		SELECT document_id, LEFT(COALESCE(content, ''), 160)
		FROM chunks
		ORDER BY created_at DESC, document_id ASC, chunk_index ASC
		LIMIT 5
	`)
	if err != nil {
		return diagnostics, err
	}
	defer chunkRows.Close()

	for chunkRows.Next() {
		var item GlobalChunkSample
		if err := chunkRows.Scan(&item.DocumentID, &item.Preview); err != nil {
			return diagnostics, err
		}
		diagnostics.Chunks = append(diagnostics.Chunks, item)
	}
	if err := chunkRows.Err(); err != nil {
		return diagnostics, err
	}

	return diagnostics, nil
}

func (r *SearchRepository) LatestGlobalChunks(ctx context.Context, limit int) ([]SearchResult, error) {
	if limit <= 0 || limit > 20 {
		limit = 5
	}

	rows, err := r.db.Query(ctx, `
		SELECT
			c.id,
			c.document_id,
			COALESCE(c.section_title, ''),
			c.content,
			c.chunk_index,
			1.0 AS score
		FROM chunks c
		INNER JOIN documents d ON d.id = c.document_id
		ORDER BY c.created_at DESC, c.document_id ASC, c.chunk_index ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]SearchResult, 0)
	for rows.Next() {
		var item SearchResult
		if err := rows.Scan(
			&item.ChunkID,
			&item.DocumentID,
			&item.SectionTitle,
			&item.Content,
			&item.ChunkIndex,
			&item.Score,
		); err != nil {
			return nil, err
		}
		results = append(results, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func globalSearchTerms(searchText string) []string {
	rawTerms := strings.Fields(normalizeRepositorySearchText(searchText))
	stopwords := map[string]bool{
		"a": true, "al": true, "algo": true, "como": true, "con": true,
		"cual": true, "cuales": true, "de": true, "del": true, "dime": true,
		"documento": true, "documentos": true, "el": true, "en": true,
		"esa": true, "ese": true, "eso": true, "esta": true, "este": true,
		"esto": true, "hay": true, "informacion": true, "la": true, "las": true,
		"lo": true, "los": true, "me": true, "mi": true, "para": true,
		"por": true, "que": true, "se": true, "segun": true, "sobre": true,
		"su": true, "sus": true, "tengo": true, "tiene": true, "un": true,
		"una": true, "y": true,
	}
	terms := make([]string, 0, len(rawTerms))
	seen := make(map[string]bool)
	for _, term := range rawTerms {
		term = strings.ToLower(strings.TrimSpace(term))
		if term == "" || stopwords[term] || seen[term] {
			continue
		}
		if len([]rune(term)) < 4 {
			continue
		}
		seen[term] = true
		terms = append(terms, term)
	}
	return terms
}

func normalizeRepositorySearchText(text string) string {
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
	return text
}

func searchTerms(searchText string) []string {
	seen := make(map[string]bool)
	terms := make([]string, 0)

	phrase := strings.TrimSpace(searchText)
	if phrase != "" {
		terms = append(terms, phrase)
		seen[strings.ToLower(phrase)] = true
	}

	for _, term := range strings.Fields(searchText) {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}

		key := strings.ToLower(term)
		if seen[key] {
			continue
		}

		seen[key] = true
		terms = append(terms, term)
	}

	return terms
}
