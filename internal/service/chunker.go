package service

import "strings"

func SplitIntoChunks(text string, maxLen int) []string {
	text = cleanExtractedText(text)
	if text == "" {
		return []string{}
	}
	if maxLen <= 0 {
		return []string{text}
	}

	blocks := splitChunkBlocks(text)
	chunks := make([]string, 0, len(blocks))
	current := ""

	appendChunk := func(chunk string) {
		chunk = cleanExtractedText(chunk)
		if chunk != "" {
			chunks = append(chunks, chunk)
		}
	}

	for _, block := range blocks {
		block = cleanExtractedText(block)
		if block == "" {
			continue
		}

		if isLikelyHeading(block) {
			if current != "" {
				appendChunk(current)
				current = ""
			}
			appendChunk(block)
			continue
		}

		if current == "" {
			if chunkLen(block) <= maxLen {
				current = block
				continue
			}
			for _, part := range splitLargeBlock(block, maxLen) {
				appendChunk(part)
			}
			continue
		}

		candidate := current + "\n\n" + block
		if chunkLen(candidate) <= maxLen {
			current = candidate
			continue
		}

		appendChunk(current)
		if chunkLen(block) <= maxLen {
			current = block
			continue
		}

		for _, part := range splitLargeBlock(block, maxLen) {
			appendChunk(part)
		}
		current = ""
	}

	if current != "" {
		appendChunk(current)
	}

	return chunks
}

func splitChunkBlocks(text string) []string {
	rawBlocks := strings.Split(text, "\n\n")
	blocks := make([]string, 0, len(rawBlocks))
	for _, block := range rawBlocks {
		block = cleanExtractedText(block)
		if block != "" {
			blocks = append(blocks, block)
		}
	}
	return blocks
}

func splitLargeBlock(block string, maxLen int) []string {
	if chunkLen(block) <= maxLen {
		return []string{block}
	}

	paragraphs := strings.Split(block, "\n")
	parts := make([]string, 0)
	current := ""

	flush := func() {
		current = cleanExtractedText(current)
		if current != "" {
			parts = append(parts, current)
		}
		current = ""
	}

	for _, paragraph := range paragraphs {
		paragraph = cleanExtractedText(paragraph)
		if paragraph == "" {
			continue
		}

		if current == "" {
			if chunkLen(paragraph) <= maxLen {
				current = paragraph
				continue
			}
			parts = append(parts, splitParagraph(paragraph, maxLen)...)
			continue
		}

		candidate := current + "\n" + paragraph
		if chunkLen(candidate) <= maxLen {
			current = candidate
			continue
		}

		flush()
		if chunkLen(paragraph) <= maxLen {
			current = paragraph
			continue
		}
		parts = append(parts, splitParagraph(paragraph, maxLen)...)
	}

	flush()
	return parts
}

func splitParagraph(paragraph string, maxLen int) []string {
	if chunkLen(paragraph) <= maxLen {
		return []string{paragraph}
	}

	sentences := splitIntoSentences(paragraph)
	parts := make([]string, 0)
	current := ""

	flush := func() {
		current = cleanExtractedText(current)
		if current != "" {
			parts = append(parts, current)
		}
		current = ""
	}

	for _, sentence := range sentences {
		sentence = cleanExtractedText(sentence)
		if sentence == "" {
			continue
		}

		if current == "" {
			if chunkLen(sentence) <= maxLen {
				current = sentence
				continue
			}
			parts = append(parts, splitByWords(sentence, maxLen)...)
			continue
		}

		candidate := current + " " + sentence
		if chunkLen(candidate) <= maxLen {
			current = candidate
			continue
		}

		flush()
		if chunkLen(sentence) <= maxLen {
			current = sentence
			continue
		}
		parts = append(parts, splitByWords(sentence, maxLen)...)
	}

	flush()
	return parts
}

func splitIntoSentences(text string) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}

	var (
		parts   []string
		builder strings.Builder
	)

	for _, r := range text {
		builder.WriteRune(r)
		if r == '.' || r == '!' || r == '?' || r == ';' {
			part := strings.TrimSpace(builder.String())
			if part != "" {
				parts = append(parts, part)
			}
			builder.Reset()
		}
	}

	if strings.TrimSpace(builder.String()) != "" {
		parts = append(parts, strings.TrimSpace(builder.String()))
	}

	if len(parts) == 0 {
		return []string{text}
	}
	return parts
}

func splitByWords(text string, maxLen int) []string {
	words := strings.Fields(text)
	if len(words) == 0 {
		return nil
	}

	parts := make([]string, 0)
	current := ""

	appendPart := func(value string) {
		value = cleanExtractedText(value)
		if value != "" {
			parts = append(parts, value)
		}
	}

	for _, word := range words {
		if current == "" {
			if chunkLen(word) <= maxLen {
				current = word
				continue
			}

			runes := []rune(word)
			for len(runes) > maxLen {
				appendPart(string(runes[:maxLen]))
				runes = runes[maxLen:]
			}
			current = string(runes)
			continue
		}

		candidate := current + " " + word
		if chunkLen(candidate) <= maxLen {
			current = candidate
			continue
		}

		appendPart(current)
		if chunkLen(word) <= maxLen {
			current = word
			continue
		}

		runes := []rune(word)
		for len(runes) > maxLen {
			appendPart(string(runes[:maxLen]))
			runes = runes[maxLen:]
		}
		current = string(runes)
	}

	appendPart(current)
	return parts
}

func chunkLen(text string) int {
	return len([]rune(text))
}
