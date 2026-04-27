package service

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"auradb-pipeline/internal/repository/postgres"
)

const (
	defaultSummarySectionTitle = "Contenido general"
	maxSummarySections         = 12
)

type SummarySectionGroup struct {
	Title      string
	DocumentID string
	Chunks     []postgres.SearchResult
	StartIndex int
	EndIndex   int
}

type SummarySectionSummary struct {
	Title      string
	Summary    string
	ChunkCount int
}

var (
	romanSectionPattern   = regexp.MustCompile(`^(?i)([ivxlcdm]+)\s*[\.\):-]\s+.+$`)
	numericSectionPattern = regexp.MustCompile(`^\d+(\.\d+){0,3}\s*[\.\):-]?\s+\S.+$`)
)

func BuildSummarySectionGroups(chunks []postgres.SearchResult) []SummarySectionGroup {
	if len(chunks) == 0 {
		return nil
	}

	sorted := append([]postgres.SearchResult(nil), chunks...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].DocumentID == sorted[j].DocumentID {
			if sorted[i].ChunkIndex == sorted[j].ChunkIndex {
				return sorted[i].ChunkID < sorted[j].ChunkID
			}
			return sorted[i].ChunkIndex < sorted[j].ChunkIndex
		}
		return sorted[i].DocumentID < sorted[j].DocumentID
	})

	groups := make([]SummarySectionGroup, 0)
	partCounter := 0

	for _, chunk := range sorted {
		detectedTitle := detectSummarySectionTitle(chunk)
		if len(groups) == 0 {
			partCounter++
			groups = append(groups, newSummarySectionGroup(chunk, coalesceSectionTitle(detectedTitle, generatedPartTitle(partCounter)), partCounter))
			continue
		}

		current := &groups[len(groups)-1]
		shouldStartNew := false

		if chunk.DocumentID != current.DocumentID {
			shouldStartNew = true
		} else if detectedTitle != "" && normalizeSummaryTitle(detectedTitle) != normalizeSummaryTitle(current.Title) {
			shouldStartNew = true
		} else if current.EndIndex >= 0 && chunk.ChunkIndex-current.EndIndex > 2 {
			shouldStartNew = true
		}

		if shouldStartNew {
			partCounter++
			title := coalesceSectionTitle(detectedTitle, generatedPartTitle(partCounter))
			groups = append(groups, newSummarySectionGroup(chunk, title, partCounter))
			continue
		}

		current.Chunks = append(current.Chunks, chunk)
		current.EndIndex = chunk.ChunkIndex
		if current.Title == defaultSummarySectionTitle || strings.HasPrefix(current.Title, "Parte ") {
			if detectedTitle != "" {
				current.Title = detectedTitle
			}
		}
	}

	groups = mergeTinySummaryGroups(groups)
	if len(groups) > maxSummarySections {
		groups = compactSummaryGroups(groups, maxSummarySections)
	}

	return groups
}

func BuildSummarySectionContext(group SummarySectionGroup) string {
	var builder strings.Builder
	builder.WriteString("Seccion: ")
	builder.WriteString(group.Title)
	builder.WriteString("\n")
	for i, chunk := range group.Chunks {
		if i > 0 {
			builder.WriteString("\n\n")
		}
		builder.WriteString("[chunk_id: ")
		builder.WriteString(chunk.ChunkID)
		builder.WriteString(" | document_id: ")
		builder.WriteString(chunk.DocumentID)
		builder.WriteString(" | section_title: ")
		builder.WriteString(coalesceSectionTitle(strings.TrimSpace(chunk.SectionTitle), group.Title))
		builder.WriteString("]\n")
		builder.WriteString(strings.TrimSpace(chunk.Content))
	}
	return builder.String()
}

func BuildSummaryOutlineText(groups []SummarySectionGroup) string {
	if len(groups) == 0 {
		return ""
	}

	var builder strings.Builder
	for i, group := range groups {
		if i > 0 {
			builder.WriteString("\n")
		}
		builder.WriteString(fmt.Sprintf("%d. %s (chunks: %d", i+1, group.Title, len(group.Chunks)))
		if group.StartIndex >= 0 && group.EndIndex >= group.StartIndex {
			builder.WriteString(fmt.Sprintf(", rango: %d-%d", group.StartIndex, group.EndIndex))
		}
		builder.WriteString(")")
	}
	return builder.String()
}

func BuildSummarySynthesisContext(groups []SummarySectionGroup, sectionSummaries []SummarySectionSummary) string {
	var builder strings.Builder
	if outline := BuildSummaryOutlineText(groups); outline != "" {
		builder.WriteString("Outline detectado:\n")
		builder.WriteString(outline)
		builder.WriteString("\n\n")
	}
	builder.WriteString("Resumenes por seccion:\n")
	for i, item := range sectionSummaries {
		if i > 0 {
			builder.WriteString("\n\n")
		}
		builder.WriteString(fmt.Sprintf("%d. %s\n", i+1, item.Title))
		builder.WriteString(strings.TrimSpace(item.Summary))
	}
	return strings.TrimSpace(builder.String())
}

func detectSummarySectionTitle(chunk postgres.SearchResult) string {
	if title := strings.TrimSpace(chunk.SectionTitle); title != "" {
		return normalizeDetectedSectionTitle(title)
	}
	lines := strings.Split(strings.ReplaceAll(chunk.Content, "\r\n", "\n"), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if looksLikeSummaryHeading(line) {
			return normalizeDetectedSectionTitle(line)
		}
	}
	return ""
}

func looksLikeSummaryHeading(line string) bool {
	if line == "" {
		return false
	}
	runes := []rune(line)
	if len(runes) > 90 {
		return false
	}
	if romanSectionPattern.MatchString(line) || numericSectionPattern.MatchString(line) {
		return true
	}
	if strings.HasSuffix(line, ":") && len(runes) <= 70 && wordCount(line) <= 8 {
		return true
	}
	if wordCount(line) >= 2 && wordCount(line) <= 10 && uppercaseRatio(line) >= 0.8 {
		return true
	}
	return false
}

func normalizeDetectedSectionTitle(title string) string {
	title = strings.TrimSpace(title)
	title = strings.Trim(title, "- ")
	title = strings.TrimSuffix(title, ":")
	return strings.Join(strings.Fields(title), " ")
}

func newSummarySectionGroup(chunk postgres.SearchResult, title string, partCounter int) SummarySectionGroup {
	title = coalesceSectionTitle(title, generatedPartTitle(partCounter))
	return SummarySectionGroup{
		Title:      title,
		DocumentID: chunk.DocumentID,
		Chunks:     []postgres.SearchResult{chunk},
		StartIndex: chunk.ChunkIndex,
		EndIndex:   chunk.ChunkIndex,
	}
}

func mergeTinySummaryGroups(groups []SummarySectionGroup) []SummarySectionGroup {
	if len(groups) < 2 {
		return groups
	}

	merged := make([]SummarySectionGroup, 0, len(groups))
	for _, group := range groups {
		if len(merged) == 0 {
			merged = append(merged, group)
			continue
		}
		prev := &merged[len(merged)-1]
		if shouldMergeSummaryGroups(*prev, group) {
			prev.Chunks = append(prev.Chunks, group.Chunks...)
			prev.EndIndex = group.EndIndex
			continue
		}
		merged = append(merged, group)
	}
	return merged
}

func shouldMergeSummaryGroups(left SummarySectionGroup, right SummarySectionGroup) bool {
	if left.DocumentID != right.DocumentID {
		return false
	}
	if len(left.Chunks) >= 2 && len(right.Chunks) >= 2 {
		return false
	}
	if right.StartIndex-left.EndIndex > 1 {
		return false
	}
	leftGenerated := strings.HasPrefix(left.Title, "Parte ") || left.Title == defaultSummarySectionTitle
	rightGenerated := strings.HasPrefix(right.Title, "Parte ") || right.Title == defaultSummarySectionTitle
	return leftGenerated || rightGenerated
}

func compactSummaryGroups(groups []SummarySectionGroup, maxGroups int) []SummarySectionGroup {
	if len(groups) <= maxGroups || maxGroups <= 0 {
		return groups
	}

	compacted := make([]SummarySectionGroup, 0, maxGroups)
	targetSize := (len(groups) + maxGroups - 1) / maxGroups
	for i := 0; i < len(groups); i += targetSize {
		end := i + targetSize
		if end > len(groups) {
			end = len(groups)
		}
		combined := groups[i]
		for j := i + 1; j < end; j++ {
			combined.Chunks = append(combined.Chunks, groups[j].Chunks...)
			combined.EndIndex = groups[j].EndIndex
			if strings.HasPrefix(combined.Title, "Parte ") && !strings.HasPrefix(groups[j].Title, "Parte ") {
				combined.Title = groups[j].Title
			}
		}
		compacted = append(compacted, combined)
	}
	return compacted
}

func coalesceSectionTitle(title string, fallback string) string {
	title = strings.TrimSpace(title)
	if title != "" {
		return title
	}
	if strings.TrimSpace(fallback) != "" {
		return fallback
	}
	return defaultSummarySectionTitle
}

func generatedPartTitle(partCounter int) string {
	return fmt.Sprintf("Parte %d", partCounter)
}

func normalizeSummaryTitle(title string) string {
	return NormalizeSearchText(title)
}

func uppercaseRatio(text string) float64 {
	letters := 0
	uppercase := 0
	for _, r := range text {
		if unicode.IsLetter(r) {
			letters++
			if unicode.IsUpper(r) {
				uppercase++
			}
		}
	}
	if letters == 0 {
		return 0
	}
	return float64(uppercase) / float64(letters)
}

func wordCount(text string) int {
	return len(strings.Fields(text))
}
