package psql

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
)

type Connection struct {
	conn          net.Conn
	reader        *bufio.Reader
	oid2typ       map[int]string
	preparedStmts map[string]string
}

func (c *Connection) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

func Connect(ctx context.Context, host string, port int, database, username, password string) (*Connection, error) {

	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))

	if err != nil {
		return nil, err
	}
	c := &Connection{
		conn:          conn,
		reader:        bufio.NewReader(conn),
		preparedStmts: make(map[string]string),
	}

	err = c.startup(database, username, password)
	if err != nil {
		return nil, err
	}

	_, data, _, _, err := c.Query("SELECT oid, typname FROM pg_type;")

	if err != nil {
		return nil, err
	}

	m := make(map[int]string)
	for _, row := range data {
		i, err := strconv.Atoi(row[0])
		if err != nil {
			continue
		}
		m[i] = row[1]
	}

	c.oid2typ = m

	return c, nil

}

// startup sends the PostgreSQL StartupMessage and handles
// authentication until AuthenticationOk is received.
func (c *Connection) startup(database string, username string, password string) error {

	// StartupMessage:
	//
	// Int32 length
	// Int32 protocol version
	// "user"     + '\0'
	// username   + '\0'
	// "database" + '\0'
	// database   + '\0'
	// '\0'
	//
	// The length includes itself.
	body := make([]byte, 0)

	body = appendInt32(body, protocolVersion)

	body = append(body, []byte("user")...)
	body = append(body, 0)
	body = append(body, []byte(username)...)
	body = append(body, 0)

	body = append(body, []byte("database")...)
	body = append(body, 0)
	body = append(body, []byte(database)...)
	body = append(body, 0)

	body = append(body, 0)

	msg := make([]byte, 4)
	binary.BigEndian.PutUint32(msg, uint32(len(body)+4))
	msg = append(msg, body...)

	if _, err := c.conn.Write(msg); err != nil {
		return err
	}

	// Authentication looc.
	for {
		msgType, payload, err := c.readMessage()
		if err != nil {
			return err
		}

		switch msgType {

		case 'R':
			// Authentication request.
			if err := c.handleAuthentication(payload, username, password); err != nil {
				return err
			}

			// handleAuthentication may have completed SCRAM
			// or sent another authentication response.

		case 'S':
			// ParameterStatus.
			//
			// Example:
			//   server_version\0
			//   16.4\0
			//
			// We don't need these parameters for this example.

		case 'K':
			// BackendKeyData.
			// Process ID + secret key.
			// Useful for cancellation, but not needed here.

		case 'Z':
			// ReadyForQuery.
			//
			// Startup/authentication is complete.
			return nil

		case 'E':
			return fmt.Errorf("postgres startup error: %s", parseError(payload))

		case 'N':
			// NoticeResponse. Ignore for this example.

		default:
			// Other startup messages can be ignored here.
		}
	}
}

// handleAuthentication processes PostgreSQL Authentication messages.
func (c *Connection) handleAuthentication(payload []byte, username string, password string) error {

	if len(payload) < 4 {
		return errors.New("invalid Authentication message")
	}

	authType := binary.BigEndian.Uint32(payload[:4])

	switch authType {

	case 0:
		// AuthenticationOk.
		return nil

	case 3:
		// AuthenticationCleartextPassword.
		//
		// PasswordMessage:
		//
		// 'p'
		// Int32 length
		// password + '\0'
		return c.sendPassword(password)

	case 5:
		// AuthenticationMD5Password.
		if len(payload) < 8 {
			return errors.New("invalid MD5 authentication message")
		}

		salt := payload[4:8]
		return c.sendMD5Password(username, password, salt)

	case 10:
		// AuthenticationSASL.
		//
		// PostgreSQL tells us which SASL mechanisms it accepts.
		//
		// Commonly:
		//
		// SCRAM-SHA-256
		//
		return c.handleSCRAM(username, password)

	default:
		return fmt.Errorf("unsupported PostgreSQL authentication type: %d", authType)
	}
}

// sendPassword sends a PasswordMessage.
func (c *Connection) sendPassword(password string) error {
	payload := append([]byte(password), 0)
	return c.sendMessage('p', payload)
}

// sendMessage writes a normal PostgreSQL frontend message.
//
// Layout:
//
//	1 byte   message type
//	4 bytes  length
//	N bytes  payload
//
// The length includes the four length bytes but does not
// include the message-type byte.
func (c *Connection) sendMessage(msgType byte, payload []byte) error {

	length := 4 + len(payload)

	buf := make([]byte, 5+len(payload))

	buf[0] = msgType

	binary.BigEndian.PutUint32(
		buf[1:5],
		uint32(length),
	)

	copy(buf[5:], payload)

	_, err := c.conn.Write(buf)

	return err
}

// readMessage reads one PostgreSQL backend message.
func (c *Connection) readMessage() (byte, []byte, error) {

	msgType, err := c.reader.ReadByte()
	if err != nil {
		return 0, nil, err
	}

	var lengthBytes [4]byte

	if _, err := io.ReadFull(
		c.reader,
		lengthBytes[:],
	); err != nil {
		return 0, nil, err
	}

	length := int(binary.BigEndian.Uint32(lengthBytes[:]))

	if length < 4 {
		return 0, nil, errors.New("invalid PostgreSQL message length")
	}

	payloadLength := length - 4

	payload := make([]byte, payloadLength)

	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return 0, nil, err
	}

	return msgType, payload, nil
}

func (c *Connection) Query(query string) (nulls [][]bool, data [][]string, columns []string, types []string, err error) {

	err = c.sendQuery(query)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	nulls, data, columns, types, err = c.readResults()
	return nulls, data, columns, types, err

}

func (c *Connection) Exec(query string, args ...any) (nulls [][]bool, data [][]string, columns []string, types []string, err error) {

	if len(args) == 0 {
		return c.Query(query)
	}

	return c.ExecPrepared(query, args...)

}

// sendQuery sends PostgreSQL's Simple Query message.
//
// Message layout:
//
//	'Q'
//	Int32 length
//	query string
//	'\0'
func (c *Connection) sendQuery(query string) error {
	payload := append([]byte(query), 0)
	return c.sendMessage('Q', payload)
}

// sendMD5Password implements PostgreSQL's MD5 password authentication.
func (c *Connection) sendMD5Password(username string, password string, salt []byte) error {

	// PostgreSQL MD5 authentication is:
	//
	// md5 + md5(password + username) + salt
	//
	// Note that this is legacy authentication. SCRAM-SHA-256
	// is preferred for modern PostgreSQL installations.

	h := md5Hash([]byte(password + username))
	first := hex.EncodeToString(h[:])

	h2 := md5Hash(append([]byte(first), salt...))

	result := "md5" + hex.EncodeToString(h2[:])

	return c.sendPassword(result)
}

// handleSCRAM performs the SCRAM-SHA-256 exchange.
func (c *Connection) handleSCRAM(username string, password string) error {

	// ---------------------------------------------------------
	// SASLInitialResponse
	// ---------------------------------------------------------

	// Generate a random client nonce.
	nonceBytes := make([]byte, 18)

	if _, err := rand.Read(nonceBytes); err != nil {
		return err
	}

	clientNonce := base64.StdEncoding.EncodeToString(nonceBytes)

	// SCRAM client-first-message:
	//
	// n,,n=<username>,r=<nonce>
	//
	// Username escaping is simplified here. For production
	// code, SASL username escaping should be implemented.
	clientFirstBare := "n=" + username + ",r=" + clientNonce

	clientFirstMessage := "n,," + clientFirstBare

	// SASLInitialResponse payload:
	//
	// mechanism\0
	// Int32 length of initial response
	// initial response
	//
	mechanism := "SCRAM-SHA-256"

	payload := make([]byte, 0)

	payload = append(payload, []byte(mechanism)...)
	payload = append(payload, 0)

	payload = appendInt32(payload, int32(len(clientFirstMessage)))
	payload = append(payload, []byte(clientFirstMessage)...)

	if err := c.sendMessage('p', payload); err != nil {
		return err
	}

	// ---------------------------------------------------------
	// AuthenticationSASLContinue
	// ---------------------------------------------------------

	msgType, serverPayload, err := c.readMessage()
	if err != nil {
		return err
	}

	if msgType != 'R' {
		return fmt.Errorf(
			"expected AuthenticationSASLContinue, got %q",
			msgType,
		)
	}

	if len(serverPayload) < 4 ||
		binary.BigEndian.Uint32(serverPayload[:4]) != 11 {

		return fmt.Errorf("expected SCRAM continue authentication")
	}

	serverFirstMessage := string(serverPayload[4:])

	// Parse:
	//
	// r=<nonce>,s=<salt>,i=<iteration-count>
	//
	parts := parseSCRAMAttributes(serverFirstMessage)

	serverNonce := parts["r"]
	saltString := parts["s"]
	iterationsString := parts["i"]

	if !strings.HasPrefix(serverNonce, clientNonce) {
		return errors.New("server nonce does not contain client nonce")
	}

	salt, err := base64.StdEncoding.DecodeString(saltString)
	if err != nil {
		return err
	}

	iterations, err := strconv.Atoi(iterationsString)
	if err != nil {
		return err
	}

	// ---------------------------------------------------------
	// Calculate SCRAM proof
	// ---------------------------------------------------------

	// client-final-without-proof:
	//
	// c=biws,r=<server nonce>
	clientFinalWithoutProof := "c=biws,r=" + serverNonce

	// AuthMessage:
	//
	// client-first-bare + "," +
	// server-first-message + "," +
	// client-final-without-proof
	authMessage := clientFirstBare + "," + serverFirstMessage + "," + clientFinalWithoutProof

	// SaltedPassword =
	//     Hi(password, salt, iterations)
	saltedPassword := pbkdf2SHA256([]byte(password), salt, iterations, 32)

	// ClientKey = HMAC(SaltedPassword, "Client Key")
	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))

	// StoredKey = SHA256(ClientKey)
	storedKey := sha256.Sum256(clientKey)

	// ClientSignature =
	//     HMAC(StoredKey, AuthMessage)
	clientSignature := hmacSHA256(storedKey[:], []byte(authMessage))

	// ClientProof = ClientKey XOR ClientSignature
	clientProof := make([]byte, len(clientKey))

	for i := range clientKey {
		clientProof[i] = clientKey[i] ^ clientSignature[i]
	}

	proof := base64.StdEncoding.EncodeToString(clientProof)

	// Final client message.
	clientFinalMessage :=
		clientFinalWithoutProof +
			",p=" + proof

	if err := c.sendMessage('p', []byte(clientFinalMessage)); err != nil {
		return err
	}

	// ---------------------------------------------------------
	// AuthenticationSASLFinal
	// ---------------------------------------------------------

	msgType, serverPayload, err = c.readMessage()
	if err != nil {
		return err
	}

	if msgType != 'R' {
		return fmt.Errorf(
			"expected AuthenticationSASLFinal, got %q",
			msgType,
		)
	}

	if len(serverPayload) < 4 ||
		binary.BigEndian.Uint32(serverPayload[:4]) != 12 {

		return fmt.Errorf("expected SCRAM final authentication")
	}

	serverFinalMessage := string(serverPayload[4:])

	finalParts := parseSCRAMAttributes(serverFinalMessage)

	serverSignature, ok := finalParts["v"]
	if !ok {
		return errors.New("server did not provide SCRAM signature")
	}

	// Verify the server signature.
	serverKey := hmacSHA256(saltedPassword, []byte("Server Key"))

	expectedServerSignature := hmacSHA256(serverKey, []byte(authMessage))

	expectedEncoded := base64.StdEncoding.EncodeToString(expectedServerSignature)

	if !hmac.Equal([]byte(serverSignature), []byte(expectedEncoded)) {
		return errors.New("SCRAM server signature verification failed")
	}

	return nil
}

// parseSCRAMAttributes parses:
//
//	n=value,n=value,...
func parseSCRAMAttributes(s string) map[string]string {

	result := make(map[string]string)

	for _, item := range strings.Split(s, ",") {

		parts := strings.SplitN(item, "=", 2)

		if len(parts) == 2 {
			result[parts[0]] = parts[1]
		}
	}

	return result
}

// readResults consumes PostgreSQL messages resulting from the query.
func (c *Connection) readResults() (nullRows [][]bool, dataRows [][]string, columns []string, types []string, err error) {

	for {
		msgType, payload, err := c.readMessage()
		if err != nil {
			return nullRows, dataRows, columns, types, err
		}

		switch msgType {

		case 'T':
			// RowDescription.
			tableHeaderDescriptors, err := parseRowDescription(payload)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			for _, t := range tableHeaderDescriptors {
				oid := t.dataTypeOid
				val := int(binary.BigEndian.Uint32(oid))
				typ, ok := c.oid2typ[val]
				if !ok {
					typ = "<unknown>"
				}
				types = append(types, typ)
				columns = append(columns, t.name)
			}

		case 'D':
			// DataRow.
			nr, dr := parseDataRow(payload)
			dataRows = append(dataRows, dr)
			nullRows = append(nullRows, nr)

		case 'C':
			// CommandComplete.

		case 'Z':
			// ReadyForQuery.
			return nullRows, dataRows, columns, types, nil

		case 'E':
			return nullRows, dataRows, columns, types, errors.New(parseError(payload))

		case 'N':
			// NoticeResponse.
			fmt.Printf(
				"Notice: %s\n",
				parseError(payload),
			)

		default:
			fmt.Printf(
				"Received message %q (%d bytes)\n",
				msgType,
				len(payload),
			)
		}
	}
}

const (
	protocolVersion = 196608 // PostgreSQL protocol 3.0
)

// printRowDescription parses a RowDescription message.

type tableRowDescription struct {
	name                  string
	tableOid              []byte
	columnAttributeNumber []byte
	dataTypeOid           []byte
	dataTypeSize          []byte
	typeModifier          []byte
	formatCode            []byte
}

func parseRowDescription(payload []byte) ([]*tableRowDescription, error) {
	if len(payload) < 2 {
		return nil, errors.New("invalid RowDescription")
	}

	count := int(binary.BigEndian.Uint16(payload[:2]))
	pos := 2

	result := make([]*tableRowDescription, 0, count)

	for i := 0; i < count; i++ {
		if pos >= len(payload) {
			return nil, fmt.Errorf(
				"RowDescription ended before column %d",
				i,
			)
		}

		end := bytesIndexZero(payload[pos:])
		if end < 0 {
			return nil, fmt.Errorf(
				"missing terminator for column %d",
				i,
			)
		}

		name := string(payload[pos : pos+end])
		pos += end + 1

		if pos+18 > len(payload) {
			return nil, fmt.Errorf("column %d has incomplete descriptor", i)
		}

		t := &tableRowDescription{
			name: name,
		}

		t.tableOid = payload[pos : pos+4]
		pos += 4

		t.columnAttributeNumber = payload[pos : pos+2]
		pos += 2

		t.dataTypeOid = payload[pos : pos+4]
		pos += 4

		t.dataTypeSize = payload[pos : pos+2]
		pos += 2

		t.typeModifier = payload[pos : pos+4]
		pos += 4

		t.formatCode = payload[pos : pos+2]
		pos += 2

		result = append(result, t)
	}

	if len(result) != count {
		return nil, fmt.Errorf(
			"RowDescription: expected %d columns, parsed %d",
			count,
			len(result),
		)
	}

	return result, nil
}

// parseDataRow parses a DataRow message.
func parseDataRow(payload []byte) ([]bool, []string) {

	if len(payload) < 2 {
		return []bool{}, []string{}
	}

	count := int(binary.BigEndian.Uint16(payload[:2]))
	pos := 2

	rowData := []string{}
	nullData := []bool{}

	for i := 0; i < count; i++ {

		if pos+4 > len(payload) {
			break
		}

		length := int32(binary.BigEndian.Uint32(payload[pos : pos+4]))
		pos += 4

		if length == -1 {
			nullData = append(nullData, true)
			rowData = append(rowData, "")
			continue
		}

		nullData = append(nullData, false)

		if pos+int(length) > len(payload) {
			break
		}

		value := string(payload[pos : pos+int(length)])
		rowData = append(rowData, value)
		pos += int(length)

	}

	return nullData, rowData

}

// parseError extracts PostgreSQL ErrorResponse/NoticeResponse fields.
func parseError(payload []byte) string {

	var fields []string

	pos := 0

	for pos < len(payload) {

		fieldType := payload[pos]
		pos++

		if fieldType == 0 {
			break
		}

		end := bytesIndexZero(payload[pos:])
		if end < 0 {
			break
		}

		value := string(payload[pos : pos+end])
		pos += end + 1

		if fieldType == 'M' {
			return value
		}

		fields = append(
			fields,
			fmt.Sprintf("%c=%s", fieldType, value),
		)
	}

	return strings.Join(fields, ", ")
}

// pbkdf2SHA256 implements PBKDF2-HMAC-SHA256.
func pbkdf2SHA256(
	password []byte,
	salt []byte,
	iterations int,
	keyLength int,
) []byte {

	var result []byte

	blockNum := uint32(1)

	for len(result) < keyLength {

		// salt || INT(blockNum)
		input := make([]byte, len(salt)+4)
		copy(input, salt)

		binary.BigEndian.PutUint32(
			input[len(salt):],
			blockNum,
		)

		u := hmacSHA256(password, input)

		t := make([]byte, len(u))
		copy(t, u)

		for i := 1; i < iterations; i++ {

			u = hmacSHA256(password, u)

			for j := range t {
				t[j] ^= u[j]
			}
		}

		result = append(result, t...)

		blockNum++
	}

	return result[:keyLength]
}

func hmacSHA256(key []byte, data []byte) []byte {

	h := hmac.New(
		sha256.New,
		key,
	)

	h.Write(data)

	return h.Sum(nil)
}

func md5Hash(data []byte) [16]byte {
	// Avoid importing crypto/md5 elsewhere in the example.
	//
	// This function is replaced below using the standard
	// crypto/md5 package.
	panic("replace md5Hash with crypto/md5 implementation")
}

func appendInt32(dst []byte, value int32) []byte {

	var b [4]byte

	binary.BigEndian.PutUint32(
		b[:],
		uint32(value),
	)

	return append(dst, b[:]...)
}

func bytesIndexZero(b []byte) int {

	for i, v := range b {
		if v == 0 {
			return i
		}
	}

	return -1
}

func (c *Connection) ExecPrepared(query string, args ...any) (nullRows [][]bool, dataRows [][]string, columns []string, types []string, err error) {

	// ---------------------------------------------------------
	// Parse
	// ---------------------------------------------------------
	//
	// Parse message:
	//
	// 'P'
	// Int32 length
	// statement name + '\0'
	// query + '\0'
	// Int16 number of parameter type OIDs
	// parameter type OIDs
	//
	// Using zero OIDs tells PostgreSQL to infer the parameter
	// types from the SQL statement.
	//

	name, exists := c.preparedStmts[query]

	if !exists {
		name = "query_" + hex.EncodeToString([]byte(query))
		if err := c.sendParse(name, query, len(args)); err != nil {
			return nil, nil, nil, nil, err
		}
		c.preparedStmts[query] = name
	}

	// ---------------------------------------------------------
	// Bind
	// ---------------------------------------------------------
	if err := c.sendBind(name, args); err != nil {
		return nil, nil, nil, nil, err
	}

	// Describe unnamed portal.
	if err := c.sendDescribePortal(); err != nil {
		return nil, nil, nil, nil, err
	}

	// ---------------------------------------------------------
	// Execute
	// ---------------------------------------------------------
	if err := c.sendExecute(); err != nil {
		return nil, nil, nil, nil, err
	}

	// ---------------------------------------------------------
	// Sync
	// ---------------------------------------------------------
	//
	// Sync tells PostgreSQL to finish this extended-query
	// cycle and return ReadyForQuery.
	//
	if err := c.sendMessage('S', nil); err != nil {
		return nil, nil, nil, nil, err
	}

	// ---------------------------------------------------------
	// Read responses
	// ---------------------------------------------------------
	return c.readExecResults()
}

func (c *Connection) sendExecute() error {

	payload := make([]byte, 0)

	// Portal name.
	// Empty = unnamed portal.
	payload = append(payload, 0)

	// Maximum number of rows.
	//
	// 0 means "no limit".
	payload = appendInt32(payload, 0)

	return c.sendMessage('E', payload)
}

func (c *Connection) sendDescribePortal() error {
	payload := make([]byte, 0, 2)

	// 'P' = portal
	payload = append(payload, 'P')

	// Empty portal name = unnamed portal.
	payload = append(payload, 0)

	return c.sendMessage('D', payload)
}

func (c *Connection) readExecResults() (nullRows [][]bool, dataRows [][]string, columns []string, types []string, err error) {
	for {
		msgType, payload, err := c.readMessage()

		if err != nil {
			return nullRows, dataRows, columns, types, err
		}

		switch msgType {

		case '1':
			// ParseComplete.

		case '2':
			// BindComplete.

		case 'T':
			// RowDescription.

			tableHeaderDescriptors, err := parseRowDescription(payload)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			columns = make([]string, 0, len(tableHeaderDescriptors))
			types = make([]string, 0, len(tableHeaderDescriptors))

			for _, descriptor := range tableHeaderDescriptors {
				oid := binary.BigEndian.Uint32(
					descriptor.dataTypeOid,
				)

				typ, ok := c.oid2typ[int(oid)]
				if !ok {
					typ = "<unknown>"
				}

				columns = append(columns, descriptor.name)

				types = append(types, typ)
			}

		case 'D':
			// DataRow.

			nullRow, dataRow := parseDataRow(payload)

			nullRows = append(nullRows, nullRow)
			dataRows = append(dataRows, dataRow)

		case 'C':
			// CommandComplete.
			//
			// For INSERT/UPDATE/DELETE there may be no
			// RowDescription/DataRow messages.

		case 'E':
			return nullRows, dataRows, columns, types,
				errors.New(parseError(payload))

		case 'N':
			// NoticeResponse.
			fmt.Printf("Notice: %s\n", parseError(payload))

		case 'n':
			// No data, ignore for now

		case 'Z':
			// ReadyForQuery.
			return nullRows, dataRows, columns, types, nil

		default:
			return nullRows, dataRows, columns, types,
				fmt.Errorf(
					"unexpected PostgreSQL message %q",
					msgType,
				)
		}
	}
}

func (c *Connection) sendParse(name string, query string, numParams int) error {

	payload := make([]byte, 0)

	// Prepared statement name.
	payload = append(payload, []byte(name)...)
	payload = append(payload, 0)

	// SQL query.
	payload = append(payload, []byte(query)...)
	payload = append(payload, 0)

	// Number of parameter type OIDs.
	payload = appendInt16(payload, int16(numParams))

	// Zero means PostgreSQL should infer the parameter type.
	for i := 0; i < numParams; i++ {
		payload = appendInt32(payload, 0)
	}

	return c.sendMessage('P', payload)
}

func appendInt16(dst []byte, value int16) []byte {
	var b [2]byte

	binary.BigEndian.PutUint16(
		b[:],
		uint16(value),
	)

	return append(dst, b[:]...)
}

func (c *Connection) sendBind(statementName string, args []any) error {

	payload := make([]byte, 0)

	// ---------------------------------------------------------
	// Portal name
	// ---------------------------------------------------------
	//
	// Empty portal name means the unnamed portal.
	//

	payload = append(payload, 0)

	// ---------------------------------------------------------
	// Prepared statement name
	// ---------------------------------------------------------
	payload = append(payload, []byte(statementName)...)
	payload = append(payload, 0)

	// ---------------------------------------------------------
	// Parameter format codes
	// ---------------------------------------------------------
	//
	// 0 = text
	// 1 = binary
	//
	// We use text for all parameters.
	//
	payload = appendInt16(payload, int16(len(args)))

	for range args {
		payload = appendInt16(payload, 0)
	}

	// ---------------------------------------------------------
	// Parameter values
	// ---------------------------------------------------------
	payload = appendInt16(payload, int16(len(args)))

	for _, arg := range args {

		if arg == nil {
			// -1 means SQL NULL.
			payload = appendInt32(payload, -1)
			continue
		}

		value, err := parameterString(arg)
		if err != nil {
			return err
		}

		payload = appendInt32(payload, int32(len(value)))

		payload = append(payload, []byte(value)...)
	}

	// ---------------------------------------------------------
	// Result format codes
	// ---------------------------------------------------------
	//
	// Zero result formats means text format by default.
	//
	payload = appendInt16(payload, 0)

	return c.sendMessage('B', payload)
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
