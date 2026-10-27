package psql

import (
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"uuid"
)

func isStructSlice(rv reflect.Value) bool {
	if !rv.IsValid() {
		return false
	}

	if rv.IsNil() {
		return false
	}

	rv = rv.Elem()

	if rv.Kind() != reflect.Slice {
		return false
	}

	return rv.Type().Elem().Kind() == reflect.Struct
}

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

	if v, ok := value.(uuid.UUID); ok {
		return v.String(), nil
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

func findFieldByTag(object reflect.Value, tagName string, tagValue string) (reflect.Value, bool) {
	structType := object.Type()

	for i := 0; i < structType.NumField(); i++ {
		sf := structType.Field(i)

		tag := sf.Tag.Get(tagName)

		if tag == tagValue {
			field := object.Field(i)

			if !field.CanSet() {
				return reflect.Value{}, false
			}

			return field, true
		}
	}

	return reflect.Value{}, false
}

func setStructValue(dst reflect.Value, fields []string, values []string, nulls []bool) error {
	if dst.Kind() != reflect.Struct {
		return fmt.Errorf(
			"expected struct, got %s",
			dst.Type(),
		)
	}

	for i, column := range fields {
		field, exists := findFieldByTag(dst, tagName, column)

		if !exists {
			continue
		}

		if !field.IsValid() {
			continue
		}

		if !field.CanSet() {
			continue
		}

		err := setStringValue(field, nulls[i], values[i])

		if err != nil {
			return fmt.Errorf("column %q: %w", column, err)
		}
	}

	return nil
}

func setStringValue(field reflect.Value, null bool, value string) error {
	// Handle NULL.
	if null {
		switch field.Kind() {
		case reflect.Pointer,
			reflect.Map,
			reflect.Slice,
			reflect.Interface:
			field.SetZero()
			return nil
		default:
			return fmt.Errorf("cannot assign NULL to %s", field.Type())
		}
	}

	// Handle *string, *int, *bool, etc.
	if field.Kind() == reflect.Pointer {
		if field.IsNil() {
			field.Set(
				reflect.New(field.Type().Elem()),
			)
		}

		return setStringValue(
			field.Elem(),
			false,
			value,
		)
	}

	switch field.Kind() {
	case reflect.String:
		field.SetString(value)
		return nil

	case reflect.Bool:
		v, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf(
				"invalid bool %q: %w",
				value,
				err,
			)
		}

		field.SetBool(v)
		return nil

	case reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64:

		v, err := strconv.ParseInt(
			value,
			10,
			field.Type().Bits(),
		)
		if err != nil {
			return fmt.Errorf(
				"invalid integer %q: %w",
				value,
				err,
			)
		}

		field.SetInt(v)
		return nil

	case reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64:

		v, err := strconv.ParseUint(
			value,
			10,
			field.Type().Bits(),
		)
		if err != nil {
			return fmt.Errorf(
				"invalid unsigned integer %q: %w",
				value,
				err,
			)
		}

		field.SetUint(v)
		return nil

	case reflect.Float32, reflect.Float64:

		v, err := strconv.ParseFloat(
			value,
			field.Type().Bits(),
		)
		if err != nil {
			return fmt.Errorf(
				"invalid float %q: %w",
				value,
				err,
			)
		}

		field.SetFloat(v)
		return nil

	case reflect.Slice:
		return setSliceValue(field, value)

	case reflect.Struct:
		if field.CanAddr() {
			if unmarshaler, ok := field.Addr().Interface().(encoding.TextUnmarshaler); ok {
				if err := unmarshaler.UnmarshalText([]byte(value)); err != nil {
					return fmt.Errorf("failed to unmarshal %s from %q: %w", field.Type(), value, err)
				}

				return nil
			}
		}

		if field.Type() == reflect.TypeOf(time.Time{}) {
			t, err := time.Parse(time.RFC3339Nano, value)
			if err != nil {
				return fmt.Errorf("invalid time %q: %w", value, err)
			}

			field.Set(reflect.ValueOf(t))
			return nil
		}
	case reflect.TypeOf(uuid.UUID{}).Kind():

		if unmarshaler, ok := field.Addr().Interface().(encoding.TextUnmarshaler); ok {
			if err := unmarshaler.UnmarshalText([]byte(value)); err != nil {
				return fmt.Errorf("failed to unmarshal %s from %q: %w", field.Type(), value, err)
			}
			return nil
		}

	}

	return fmt.Errorf("unsupported destination type %s", field.Type())
}

func setSliceValue(field reflect.Value, value string) error {
	// PostgreSQL arrays normally look like:
	//
	// {foo,bar,baz}
	// {1,2,3}
	// {true,false,true}

	if len(value) < 2 ||
		value[0] != '{' ||
		value[len(value)-1] != '}' {
		return fmt.Errorf(
			"invalid array value %q",
			value,
		)
	}

	value = value[1 : len(value)-1]

	// Empty PostgreSQL array: {}
	if value == "" {
		field.Set(
			reflect.MakeSlice(
				field.Type(),
				0,
				0,
			),
		)
		return nil
	}

	values, err := parsePostgresArray(value)
	if err != nil {
		return err
	}

	result := reflect.MakeSlice(
		field.Type(),
		len(values),
		len(values),
	)

	for i, value := range values {
		if err := setStringValue(
			result.Index(i),
			false,
			value,
		); err != nil {
			return fmt.Errorf(
				"invalid element %d of %s: %w",
				i,
				field.Type(),
				err,
			)
		}
	}

	field.Set(result)

	return nil
}
