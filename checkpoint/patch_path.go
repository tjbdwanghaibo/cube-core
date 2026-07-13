package checkpoint

import (
	"strconv"
	"strings"
)

func MapPatchPath(field string, key any) (string, bool) {
	if field == "" {
		return "", false
	}
	keyText, ok := mapPatchKeyString(key)
	if !ok || !safeMongoPathSegment(keyText) {
		return "", false
	}
	return field + "." + keyText, true
}

func PersistPatchHasPath(set map[string]any, unset []string, field string) bool {
	prefix := field + "."
	for path := range set {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	for _, path := range unset {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func PersistPatchPathCovered(path string, fullFields map[string]bool) bool {
	if len(fullFields) == 0 {
		return false
	}
	field := path
	if idx := strings.IndexByte(path, '.'); idx >= 0 {
		field = path[:idx]
	}
	return fullFields[field]
}

func mapPatchKeyString(key any) (string, bool) {
	switch v := key.(type) {
	case string:
		return v, v != ""
	case int:
		return strconv.FormatInt(int64(v), 10), true
	case int8:
		return strconv.FormatInt(int64(v), 10), true
	case int16:
		return strconv.FormatInt(int64(v), 10), true
	case int32:
		return strconv.FormatInt(int64(v), 10), true
	case int64:
		return strconv.FormatInt(v, 10), true
	case uint:
		return strconv.FormatUint(uint64(v), 10), true
	case uint8:
		return strconv.FormatUint(uint64(v), 10), true
	case uint16:
		return strconv.FormatUint(uint64(v), 10), true
	case uint32:
		return strconv.FormatUint(uint64(v), 10), true
	case uint64:
		return strconv.FormatUint(v, 10), true
	default:
		return "", false
	}
}

func safeMongoPathSegment(segment string) bool {
	return segment != "" && !strings.Contains(segment, ".") && !strings.HasPrefix(segment, "$")
}
