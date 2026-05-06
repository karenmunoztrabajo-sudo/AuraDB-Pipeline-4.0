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
	if limit <= 0 || limit > 20 {
		limit = 20
	}

	terms := globalSearchTerms(searchText)
	if len(terms) == 0 {
		return nil, nil
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
	args := []any{tenantID}
	nextArg := 2
	for i, term := range terms {
		if i > 0 {
			query += " + "
		}
		query += "CASE WHEN c.content ILIKE $" + strconv.Itoa(nextArg) + " THEN 1 ELSE 0 END"
		args = append(args, "%"+term+"%")
		nextArg++
	}
	query += `
			)::float AS score
		FROM chunks c
		INNER JOIN documents d ON d.id = c.document_id
		WHERE d.tenant_id = $1
		  AND (
	`
	firstTermArg := 2
	for i := range terms {
		if i > 0 {
			query += " OR "
		}
		query += "c.content ILIKE $" + strconv.Itoa(firstTermArg+i)
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

func globalSearchTerms(searchText string) []string {
	rawTerms := searchTerms(searchText)
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
