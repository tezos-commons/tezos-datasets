package parser

import (
	"github.com/Jeffail/gabs/v2"
)

// ValueCallback is called for each leaf value found in the JSON tree
type ValueCallback func(path string, value interface{})

// IterateAllValues recursively walks the JSON tree and calls the callback for each leaf value
func IterateAllValues(container *gabs.Container, callback ValueCallback) {
	iterateRecursive(container, "", callback)
}

// iterateRecursive walks the JSON tree recursively
func iterateRecursive(container *gabs.Container, path string, callback ValueCallback) {
	if container == nil {
		return
	}

	data := container.Data()
	if data == nil {
		callback(path, nil)
		return
	}

	switch v := data.(type) {
	case map[string]interface{}:
		// Object - iterate over children
		children := container.ChildrenMap()
		for key, child := range children {
			childPath := key
			if path != "" {
				childPath = path + "." + key
			}
			iterateRecursive(child, childPath, callback)
		}
	case []interface{}:
		// Array - iterate over elements
		children := container.Children()
		for i, child := range children {
			var childPath string
			if path != "" {
				childPath = path + "[]"
			} else {
				childPath = "[]"
			}
			_ = i // Index available if needed
			iterateRecursive(child, childPath, callback)
		}
	default:
		// Leaf value (string, number, bool, null)
		callback(path, v)
	}
}

// ParseJSON parses JSON bytes into a gabs Container
func ParseJSON(data []byte) (*gabs.Container, error) {
	return gabs.ParseJSON(data)
}

// ExtractStrings extracts all string values from the JSON tree
func ExtractStrings(container *gabs.Container) []string {
	var strings []string
	IterateAllValues(container, func(path string, value interface{}) {
		if s, ok := value.(string); ok {
			strings = append(strings, s)
		}
	})
	return strings
}
