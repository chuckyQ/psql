package psql

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Host            string
	Port            int
	User            string
	Password        string
	Database        string
	SSLMode         string
	SSLCert         string
	SSLKey          string
	SSLRootCert     string
	ApplicationName string
	Options         map[string]string
	DSN             string
	MaxConns        uint
	MinConns        uint
	MaxIdleConns    uint
	MinIdleConns    uint
	IdleConnTimeout time.Duration
	ConnTimeout     time.Duration
	QueryWaitTime   time.Duration
}

func parsePostgresDSN(dsn string) (*Config, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil, fmt.Errorf("DSN cannot be empty")
	}

	cfg := &Config{
		Host:    "localhost",
		Port:    5432,
		SSLMode: "prefer",
		Options: make(map[string]string),
	}

	var params map[string]string

	if strings.HasPrefix(dsn, "postgres://") ||
		strings.HasPrefix(dsn, "postgresql://") {
		var err error
		params, err = parseURLDSN(dsn)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		params, err = parseKeywordDSN(dsn)
		if err != nil {
			return nil, err
		}
	}

	for key, value := range params {
		switch key {
		case "host":
			cfg.Host = value
		case "port":
			port, err := strconv.Atoi(value)
			if err != nil || port < 1 || port > 65535 {
				return nil, fmt.Errorf("invalid port %q", value)
			}
			cfg.Port = port
		case "user":
			cfg.User = value
		case "password":
			cfg.Password = value
		case "dbname":
			cfg.Database = value
		case "sslmode":
			cfg.SSLMode = value
		case "sslcert":
			cfg.SSLCert = value
		case "sslkey":
			cfg.SSLKey = value
		case "sslrootcert":
			cfg.SSLRootCert = value
		case "connect_timeout":
			timeout, err := strconv.Atoi(value)
			if err != nil || timeout < 0 {
				return nil, fmt.Errorf(
					"invalid connect_timeout %q", value,
				)
			}
			cfg.ConnectTimeout = timeout
		case "application_name":
			cfg.ApplicationName = value
		default:
			cfg.Options[key] = value
		}
	}

	return cfg, nil
}

func parseKeywordDSN(dsn string) (map[string]string, error) {
	params := make(map[string]string)
	i := 0

	for i < len(dsn) {
		// Skip whitespace.
		for i < len(dsn) && isSpace(dsn[i]) {
			i++
		}

		if i == len(dsn) {
			break
		}

		// Read key.
		start := i
		for i < len(dsn) &&
			dsn[i] != '=' &&
			!isSpace(dsn[i]) {
			i++
		}

		if i == start || i >= len(dsn) || dsn[i] != '=' {
			return nil, fmt.Errorf(
				"expected key=value near %q", dsn[start:],
			)
		}

		key := dsn[start:i]
		i++ // Skip '='.

		var value strings.Builder

		if i < len(dsn) && dsn[i] == '\'' {
			i++ // Skip opening quote.
			closed := false

			for i < len(dsn) {
				c := dsn[i]
				i++

				if c == '\\' {
					if i >= len(dsn) {
						return nil, fmt.Errorf(
							"trailing escape in %q", key,
						)
					}
					value.WriteByte(dsn[i])
					i++
					continue
				}

				if c == '\'' {
					closed = true
					break
				}

				value.WriteByte(c)
			}

			if !closed {
				return nil, fmt.Errorf(
					"unterminated quoted value for %q", key,
				)
			}

			if i < len(dsn) && !isSpace(dsn[i]) {
				return nil, fmt.Errorf(
					"unexpected character after %q", key,
				)
			}
		} else {
			for i < len(dsn) && !isSpace(dsn[i]) {
				if dsn[i] == '\\' {
					i++
					if i >= len(dsn) {
						return nil, fmt.Errorf(
							"trailing escape in %q", key,
						)
					}
				}

				value.WriteByte(dsn[i])
				i++
			}
		}

		params[key] = value.String()
	}

	return params, nil
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' ||
		c == '\n' || c == '\r'
}

func parseURLDSN(dsn string) (map[string]string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse DSN URL: %w", err)
	}

	params := make(map[string]string)

	if u.User != nil {
		params["user"] = u.User.Username()

		if password, ok := u.User.Password(); ok {
			params["password"] = password
		}
	}

	if host := u.Hostname(); host != "" {
		params["host"] = host
	}

	if port := u.Port(); port != "" {
		params["port"] = port
	}

	if db := strings.TrimPrefix(u.Path, "/"); db != "" {
		decoded, err := url.PathUnescape(db)
		if err != nil {
			return nil, fmt.Errorf("decode database name: %w", err)
		}
		params["dbname"] = decoded
	}

	for key, values := range u.Query() {
		if len(values) > 0 {
			params[key] = values[len(values)-1]
		}
	}

	return params, nil
}
