package service

import "strings"

const mandatoryInterpretationRule = "No copies el texto del documento.\nInterpreta y explica el contenido con tus propias palabras."

func ensureInterpretationRule(userRules string) string {
	if strings.Contains(userRules, "No copies el texto del documento") {
		return userRules
	}
	userRules = strings.TrimSpace(userRules)
	if userRules == "" {
		return mandatoryInterpretationRule
	}
	return userRules + "\n- " + strings.ReplaceAll(mandatoryInterpretationRule, "\n", "\n- ")
}
