package psql

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
)

type Client struct {
	pool   *Pool
	config *Config
}

const tagName = "psql"

func (c *Client) Query(ctx context.Context, query string, out any, args ...any) error {

	if out == nil {
		return errors.New("out cannot be nil")
	}

	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return errors.New("out must be a non-nil pointer to a slice of structs")
	}

	dst := rv.Elem()
	if dst.Kind() != reflect.Slice || dst.Type().Elem().Kind() != reflect.Struct {
		return errors.New("out must be a non-nil pointer to a slice of structs")
	}

	id, conn, err := c.pool.getConn(ctx)
	if err != nil {
		return err
	}
	defer c.pool.release(id, conn)

	nulls, data, columns, _, err := conn.ExecPrepared(ctx, query, args...)
	if err != nil {
		return err
	}

	if len(nulls) != len(data) {
		return fmt.Errorf("result has %d rows but %d null rows", len(data), len(nulls))
	}

	objects := reflect.MakeSlice(dst.Type(), len(data), len(data))

	for i, row := range data {
		if len(row) != len(columns) || len(nulls[i]) != len(columns) {
			return fmt.Errorf(
				"row %d has %d values and %d null flags, expected %d columns",
				i, len(row), len(nulls[i]), len(columns),
			)
		}

		object := objects.Index(i)

		for j, value := range row {
			column := columns[j]
			field, ok := findFieldByTag(object, tagName, column)
			if !ok {
				continue
			}

			if err := setStringValue(field, nulls[i][j], value); err != nil {
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
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return errors.New("out must be a non-nil pointer")
	}

	dst := rv.Elem()

	// Allocate intermediate pointers where necessary.
	for dst.Kind() == reflect.Pointer {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		dst = dst.Elem()
	}

	id, conn, err := c.pool.getConn(ctx)
	if err != nil {
		return err
	}
	defer c.pool.release(id, conn)

	nulls, data, columns, _, err := conn.ExecPrepared(ctx, query, args...)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return errors.New("zero results")
	}
	if len(data) > 1 {
		return fmt.Errorf("expected single result, got %d results", len(data))
	}
	if len(nulls) != len(data) {
		return errors.New("result row/null metadata mismatch")
	}

	row := data[0]
	null := nulls[0]

	if len(row) != len(columns) || len(null) != len(columns) {
		return errors.New("result column metadata mismatch")
	}

	if dst.Kind() == reflect.Struct {
		return setStructValue(dst, columns, row, null)
	}

	if len(row) != 1 {
		return fmt.Errorf(
			"expected single column for %s, got %d",
			dst.Type(), len(row),
		)
	}

	return setStringValue(dst, null[0], row[0])
}

func (c *Client) QueryRaw(ctx context.Context, query string, args ...any) (nulls [][]bool, data [][]string, columns []string, types []string, err error) {

	id, conn, err := c.pool.getConn(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	defer c.pool.release(id, conn)

	return conn.ExecPrepared(ctx, query, args...)
}

func (c *Client) QuerySingleRaw(ctx context.Context, query string, args ...any) (nulls []bool, data []string, columns []string, types []string, err error) {

	id, conn, err := c.pool.getConn(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	defer c.pool.release(id, conn)

	nullRows, dataRows, columns, types, err := conn.ExecPrepared(ctx, query, args...)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if len(dataRows) == 0 {
		return nil, nil, nil, nil, errors.New("zero results")
	}
	if len(dataRows) > 1 {
		return nil, nil, nil, nil, fmt.Errorf(
			"expected single result, got %d results",
			len(dataRows),
		)
	}
	if len(nullRows) != len(dataRows) {
		return nil, nil, nil, nil, errors.New("result row/null metadata mismatch")
	}

	return nullRows[0], dataRows[0], columns, types, nil
}

func NewClient(ctx context.Context, config Config) (*Client, error) {

	defaultConfig := Config{
		Host:            "localhost",
		Port:            5432,
		User:            "postgres",
		Password:        "",
		Database:        "postgres",
		SSLMode:         "",
		SSLCert:         "",
		SSLKey:          "",
		SSLRootCert:     "",
		ApplicationName: "",
		Options:         make(map[string]string),
		DSN:             "",
		MinConns:        1,
		MaxConns:        3,
		IdleConnTimeout: time.Duration(30 * time.Second),
		MaxIdleConns:    1,
		ConnTimeout:     time.Duration(30 * time.Second),
		MinIdleConns:    1,
	}

	if config.Host != "" {
		defaultConfig.Host = config.Host
	}

	if config.Port > 0 {
		defaultConfig.Port = config.Port
	}

	if config.User != "" {
		defaultConfig.User = "postgres"
	}

	if config.Password != "" {
		defaultConfig.Password = "postgres"
	}

	if config.Database != "" {
		defaultConfig.Database = "postgres"
	}

	if config.SSLMode != "" {
		defaultConfig.SSLMode = config.SSLMode
	}
	if config.SSLCert != "" {
		defaultConfig.SSLCert = config.SSLCert
	}
	if config.SSLKey != "" {
		defaultConfig.SSLKey = config.SSLKey
	}
	if config.SSLRootCert != "" {
		defaultConfig.SSLRootCert = config.SSLRootCert
	}
	if config.ConnTimeout > 0 {
		defaultConfig.ConnTimeout = config.ConnTimeout
	}
	if config.ApplicationName != "" {
		defaultConfig.ApplicationName = config.ApplicationName
	}
	if config.Options != nil {
		defaultConfig.Options = config.Options
	}
	if config.DSN != "" {
		c, err := parsePostgresDSN(config.DSN)
		if err != nil {
			return nil, err
		}
		defaultConfig = *c
	}

	if config.MaxConns > 0 {
		defaultConfig.MaxConns = config.MaxConns
	}

	if config.MinConns > 0 {
		defaultConfig.MinConns = config.MinConns
	}

	if config.MaxConns < config.MinConns {
		return nil, errors.New("max connections cannot be < min connections")
	}

	if config.MaxIdleConns > 0 {
		defaultConfig.MaxIdleConns = config.MaxIdleConns
	}

	if config.IdleConnTimeout > 0 {
		defaultConfig.IdleConnTimeout = config.IdleConnTimeout
	}

	if config.ConnTimeout > 0 {
		defaultConfig.ConnTimeout = config.ConnTimeout
	}

	if config.MinIdleConns > config.MaxIdleConns {
		return nil, errors.New("min idle connections cannot be > max idle connections")
	}

	c := &Client{
		pool:   newPool(&config),
		config: &defaultConfig,
	}

	return c, nil

}
