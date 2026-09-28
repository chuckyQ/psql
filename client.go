package psql

import (
	"context"
	"encoding"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	conn *Connection
}

const tagName = "psql"

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

func (c *Client) Query(ctx context.Context, query string, out any, args ...any) error {

	if out == nil {
		return errors.New("out cannot be nil")
	}

	rv := reflect.ValueOf(out)

	if !isStructSlice(rv) {
		return errors.New(
			"out must be a non-nil pointer to a slice of structs",
		)
	}

	nulls, data, columns, _, err := c.conn.ExecPrepared(ctx, query, args...)
	if err != nil {
		return err
	}

	dst := rv.Elem()

	objects := reflect.MakeSlice(dst.Type(), len(data), len(data))

	for i, row := range data {
		object := objects.Index(i)

		if len(row) != len(columns) {
			return fmt.Errorf("row %d has %d values, expected %d", i, len(row), len(columns))
		}

		for j, value := range row {
			column := columns[j]
			isNull := nulls[i][j]

			field, ok := findFieldByTag(object, tagName, column)

			if !ok {
				// Ignore columns that don't have a matching
				// struct field.
				continue
			}

			if err := setStringValue(field, isNull, value); err != nil {
				return fmt.Errorf("row %d, column %q: %w", i, column, err)
			}
		}
	}

	dst.Set(objects)

	return nil
}

func (c *Client) QuerySingle(ctx context.Context, query string, out any, args ...any) error {
	if out == nil {
		return errors.New("out cannot be nil")
	}

	rv := reflect.ValueOf(out)

	if rv.IsNil() {
		return errors.New("output cannot be nil")
	}

	// Unwrap pointers.
	dst := rv
	for dst.Kind() == reflect.Pointer {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}

		dst = dst.Elem()
	}

	nulls, data, fields, _, err := c.conn.ExecPrepared(ctx, query, args...)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return errors.New("zero results")
	}

	if len(data) > 1 {
		return fmt.Errorf(
			"expected single result, got %d results",
			len(data),
		)
	}

	row := data[0]
	null := nulls[0]

	// Handle struct / pointer-to-struct.
	if dst.Kind() == reflect.Struct {
		return setStructValue(dst, fields, row, null)
	}

	// Handle scalar / pointer-to-scalar.
	if len(row) != 1 {
		return fmt.Errorf(
			"expected single column for %s, got %d",
			dst.Type(),
			len(row),
		)
	}

	return setStringValue(dst, null[0], row[0])
}

func (c *Client) QueryRaw(ctx context.Context, query string, args ...any) (nulls [][]bool, data [][]string, columns []string, types []string, err error) {
	return c.conn.ExecPrepared(ctx, query, args...)
}

func (c *Client) QuerySingleRaw(ctx context.Context, query string, args ...any) (nulls []bool, data []string, columns []string, types []string, err error) {

	nulls_, data_, columns, types, err := c.conn.ExecPrepared(ctx, query, args...)
	if err != nil {
		return
	}

	if len(data_) == 0 {
		return nil, nil, nil, nil, errors.New("zero results")
	}

	if len(data_) > 1 {
		return nil, nil, nil, nil, fmt.Errorf(
			"expected single result, got %d results",
			len(data),
		)
	}

	row := data_[0]
	null := nulls_[0]

	return null, row, columns, types, err
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

func NewClient(ctx context.Context, host string, port int, database, username, password string) (*Client, error) {

	conn, err := Connect(ctx, host, port, database, username, password)
	if err != nil {
		return nil, err
	}

	c := &Client{
		conn: conn,
	}

	return c, nil

}
