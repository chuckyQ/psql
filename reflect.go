package psql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func parameterString(value any) (string, error) {

	if value == nil {
		return "", nil
	}

	rv := reflect.ValueOf(value)

	// Dereference pointers.
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return "", nil
		}

		rv = rv.Elem()
	}

	// Handle slices and arrays.
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		var b strings.Builder

		b.WriteByte('{')

		for i := 0; i < rv.Len(); i++ {
			if i > 0 {
				b.WriteByte(',')
			}

			element := rv.Index(i)

			// Handle interface{} elements.
			if element.Kind() == reflect.Interface {
				if element.IsNil() {
					b.WriteString("NULL")
					continue
				}

				element = element.Elem()
			}

			s, err := parameterString(element.Interface())
			if err != nil {
				return "", err
			}

			b.WriteString(s)
		}

		b.WriteByte('}')

		return b.String(), nil
	}

	value = rv.Interface()

	switch v := value.(type) {

	case string:
		return v, nil

	case []byte:
		return string(v), nil

	case bool:
		if v {
			return "true", nil
		}
		return "false", nil

	case int:
		return strconv.Itoa(v), nil

	case int8:
		return strconv.FormatInt(int64(v), 10), nil

	case int16:
		return strconv.FormatInt(int64(v), 10), nil

	case int32:
		return strconv.FormatInt(int64(v), 10), nil

	case int64:
		return strconv.FormatInt(v, 10), nil

	case uint:
		return strconv.FormatUint(uint64(v), 10), nil

	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil

	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil

	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil

	case uint64:
		return strconv.FormatUint(v, 10), nil

	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32), nil

	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), nil

	case fmt.Stringer:
		return v.String(), nil

	default:
		return "", fmt.Errorf(
			"unsupported PostgreSQL parameter type %T",
			value,
		)
	}
}

func parsePostgresArray(value string) ([]string, error) {
	var result []string
	var current strings.Builder

	inQuotes := false
	escaped := false

	for _, r := range value {

		if escaped {
			current.WriteRune(r)
			escaped = false
			continue
		}

		if r == '\\' && inQuotes {
			escaped = true
			continue
		}

		switch r {
		case '"':
			inQuotes = !inQuotes

		case ',':
			if !inQuotes {
				result = append(
					result,
					current.String(),
				)
				current.Reset()
				continue
			}

			current.WriteRune(r)

		default:
			current.WriteRune(r)
		}
	}

	if inQuotes {
		return nil, fmt.Errorf(
			"unterminated quoted array element",
		)
	}

	result = append(
		result,
		current.String(),
	)

	return result, nil
}
