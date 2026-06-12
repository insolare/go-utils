//  This file is part of the eliona project.
//  Copyright © 2022 LEICOM iTEC AG. All Rights Reserved.
//  ______ _ _
// |  ____| (_)
// | |__  | |_  ___  _ __   __ _
// |  __| | | |/ _ \| '_ \ / _` |
// | |____| | | (_) | | | | (_| |
// |______|_|_|\___/|_| |_|\__,_|
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
//  BUT NOT LIMITED  TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
//  NON INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
//  DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eliona-smart-building-assistant/go-utils/common"
	"github.com/eliona-smart-building-assistant/go-utils/log"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/volatiletech/sqlboiler/v4/drivers/sqlboiler-psql/driver"
)

// ConnectionString returns the connection string defined in the environment variable CONNECTION_STRING.
func ConnectionString() string {
	return common.Getenv("CONNECTION_STRING", "")
}

// InitConnectionString returns the connection string for init defined in the environment variable INIT_CONNECTION_STRING. Default is value CONNECTION_STRING.
func InitConnectionString() string {
	return common.Getenv("INIT_CONNECTION_STRING", ConnectionString())
}

// Hostname returns the defined hostname configured in CONNECTION_STRING
func Hostname() string {
	connectionStringUrl := connectionStringUrl()
	if connectionStringUrl != nil {
		return connectionStringUrl.Hostname()
	}
	return ""
}

func InitHostname() string {
	connectionStringUrl := initConnectionStringUrl()
	if connectionStringUrl != nil {
		return connectionStringUrl.Hostname()
	}
	return ""
}

func port(connectionStringUrl *url.URL) int {
	if connectionStringUrl != nil {
		port, err := strconv.Atoi(connectionStringUrl.Port())
		if err == nil {
			return port
		}
	}
	return 0
}

// Port returns the defined port configured in CONNECTION_STRING
func Port() int {
	return port(connectionStringUrl())
}

// InitPort returns the defined port configured in INIT_CONNECTION_STRING
func InitPort() int {
	return port(initConnectionStringUrl())
}

func username(connectionStringUrl *url.URL) string {
	if connectionStringUrl != nil {
		return connectionStringUrl.User.Username()
	}
	return ""
}

// Username returns the defined username configured in CONNECTION_STRING
func Username() string {
	return username(connectionStringUrl())
}

// InitUsername returns the defined username configured in INIT_CONNECTION_STRING
func InitUsername() string {
	return username(initConnectionStringUrl())
}

func password(connectionStringUrl *url.URL) string {
	if connectionStringUrl != nil {
		password, exists := connectionStringUrl.User.Password()
		if exists {
			return password
		}
	}
	return ""
}

// Password returns the defined password configured in CONNECTION_STRING
func Password() string {
	return password(connectionStringUrl())
}

// InitPassword returns the defined password configured in INIT_CONNECTION_STRING
func InitPassword() string {
	return password(initConnectionStringUrl())
}

func databaseName(connectionStringUrl *url.URL) string {
	if connectionStringUrl != nil && len(connectionStringUrl.Path) > 1 {
		return connectionStringUrl.Path[1:]
	}
	return common.Getenv("PGDATABASE", "")
}

// DatabaseName returns the defined database name configured in CONNECTION_STRING
func DatabaseName() string {
	return databaseName(connectionStringUrl())
}

// InitDatabaseName returns the defined database name configured in INIT_CONNECTION_STRING
func InitDatabaseName() string {
	return databaseName(initConnectionStringUrl())
}

func connectionStringUrl() *url.URL {
	parse, err := url.Parse(ConnectionString())
	if err != nil {
		return nil
	}
	return parse
}

func initConnectionStringUrl() *url.URL {
	parse, err := url.Parse(InitConnectionString())
	if err != nil {
		return nil
	}
	return parse
}

// The Connection interface allows mocking database connection for testing
type Connection interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	Begin(ctx context.Context) (pgx.Tx, error)
}

func GetConnectionConfig(conn *Connection) *pgx.ConnConfig {
	if conn == nil {
		return nil
	}
	if pgxConn, ok := (*conn).(*pgx.Conn); ok {
		if pgxConn != nil {
			return pgxConn.Config()
		}
	} else if pgxPool, ok := (*conn).(*pgxpool.Pool); ok {
		if pgxPool != nil && pgxPool.Config() != nil {
			return pgxPool.Config().ConnConfig
		}
	} else if pgxTx, ok := (*conn).(pgx.Tx); ok {
		if pgxTx.Conn() != nil {
			return pgxTx.Conn().Config()
		}
	}
	return nil
}

// ConnectionConfig returns the connection config defined by CONNECTION_STRING environment variable.
func ConnectionConfig() *pgx.ConnConfig {
	return connectionConfig(ConnectionString())
}

// InitConnectionConfig returns the connection config defined by INIT_CONNECTION_STRING environment variable.
func InitConnectionConfig() *pgx.ConnConfig {
	return connectionConfig(InitConnectionString())
}

func connectionConfig(connectionString string) *pgx.ConnConfig {
	config, err := pgx.ParseConfig(connectionString)
	if err != nil {
		log.Fatal("Database", "Unable to parse database URL: %v", err)
	}
	return config
}

func ConnectionConfigWithApplicationName(applicationName string) *pgx.ConnConfig {
	return connectionConfigWithApplicationName(ConnectionString(), applicationName)
}

func InitConnectionConfigWithApplicationName(applicationName string) *pgx.ConnConfig {
	return connectionConfigWithApplicationName(InitConnectionString(), applicationName)
}

func connectionConfigWithApplicationName(connectionString string, applicationName string) *pgx.ConnConfig {
	config := connectionConfig(connectionString)
	config.RuntimeParams = map[string]string{
		"application_name": applicationName,
	}
	return config
}

func PoolConfig() *pgxpool.Config {
	return poolConfig(ConnectionString())
}

func InitPoolConfig() *pgxpool.Config {
	return poolConfig(InitConnectionString())
}

func poolConfig(connectionString string) *pgxpool.Config {
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		log.Fatal("Database", "Unable to parse database URL: %v", err)
	}
	return config
}

func ExecFile(connection Connection, path string) error {
	sql, err := ioutil.ReadFile(filepath.Join(path))
	if err != nil {
		log.Error("Database", "Unable to read sql file %s: %v", path, err)
		return err
	}
	_, err = connection.Exec(context.Background(), string(sql))
	if err != nil {
		log.Error("Database", "Error during execute sql file %s: %v", path, err)
		return err
	}
	return nil
}

// NewConnection returns a new connection defined by CONNECTION_STRING environment variable.
func NewConnection() *pgx.Conn {
	return NewConnectionWithContext(context.Background())
}

// NewInitConnection returns a new connection defined by INIT_CONNECTION_STRING environment variable.
func NewInitConnection() *pgx.Conn {
	return NewInitConnectionWithContext(context.Background())
}

// NewConnectionWithContext returns a new connection defined by CONNECTION_STRING environment variable.
func NewConnectionWithContext(ctx context.Context) *pgx.Conn {
	return newConnectionWithContext(ctx, ConnectionConfig())
}

// NewInitConnectionWithContext returns a new connection defined by INIT_CONNECTION_STRING environment variable.
func NewInitConnectionWithContext(ctx context.Context) *pgx.Conn {
	return newConnectionWithContext(ctx, InitConnectionConfig())
}

// NewConnectionWithContext returns a new connection defined by CONNECTION_STRING environment variable.
func newConnectionWithContext(ctx context.Context, config *pgx.ConnConfig) *pgx.Conn {
	connection, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		log.Fatal("Database", "Unable to create connection to database: %v", err)
	}
	log.Debug("Database", "Connection created")
	return connection
}

func NewConnectionWithContextAndApplicationName(ctx context.Context, applicationName string) *pgx.Conn {
	return newConnectionWithContextAndApplicationName(ctx, ConnectionConfigWithApplicationName(applicationName))
}

func NewInitConnectionWithContextAndApplicationName(ctx context.Context, applicationName string) *pgx.Conn {
	return newConnectionWithContextAndApplicationName(ctx, InitConnectionConfigWithApplicationName(applicationName))
}

func newConnectionWithContextAndApplicationName(ctx context.Context, config *pgx.ConnConfig) *pgx.Conn {
	connection, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		log.Fatal("Database", "Unable to create connection to database: %v", err)
	}
	log.Debug("Database", "Connection created")
	return connection
}

func NewPool() *pgxpool.Pool {
	return newPool(PoolConfig())
}

func NewInitPool() *pgxpool.Pool {
	return newPool(InitPoolConfig())
}

func newPool(config *pgxpool.Config) *pgxpool.Pool {
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		log.Fatal("Database", "Unable to create pool for database: %v", err)
	}
	log.Debug("Database", "Pool created")
	return pool
}

// current holds a single connection
var poolMutex sync.Mutex
var pool *pgxpool.Pool

// Pool returns the default pool hold by this package. The pool is created if this function is called first time.
// Afterward this function returns always the same pool. Don't forget to defer the pool with ClosePool function.
func Pool() *pgxpool.Pool {
	if pool == nil {
		poolMutex.Lock()
		if pool == nil {
			pool = NewPool()
		}
		poolMutex.Unlock()
	}
	return pool
}

// ClosePool closes the default pool hold by this package.
func ClosePool() {
	if pool != nil {
		pool.Close()
		pool = nil
	}
}

// current holds a single connection
var databaseMutex sync.Mutex
var database *sql.DB

// Database returns the configured database connection from CONNECTION_STRING. If once opened this method returns always the same database.
func Database(applicationName string) *sql.DB {
	if database == nil {
		databaseMutex.Lock()
		if database == nil {
			database = NewDatabase(applicationName)
		}
		databaseMutex.Unlock()
	}
	return database
}

// DatabaseWithContext returns the configured database connection from CONNECTION_STRING. If once opened this method returns always the same database.
//
// Context will be used only during connection check when new database instance is created.
func DatabaseWithContext(ctx context.Context, applicationName string) *sql.DB {
	if database == nil {
		databaseMutex.Lock()
		if database == nil {
			database = NewDatabaseWithContext(ctx, applicationName)
		}
		databaseMutex.Unlock()
	}
	return database
}

func newDatabase(ctx context.Context, connectionString string) *sql.DB {
	database, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal("Database", "Cannot connect to database: %v", err)
	}
	err = database.PingContext(ctx)
	if err != nil {
		log.Debug("Database", "Try Database connection without SSL: %v", err)
		database, err = sql.Open("postgres", connectionString+" sslmode=disable")
		if err != nil {
			log.Fatal("Database", "Cannot connect to database: %v", err)
		}
		err = database.PingContext(ctx)
		if err != nil {
			log.Fatal("Database", "Cannot connect to database: %v", err)
		}
	}
	log.Debug("Database", "Database created")
	return database
}

// NewDatabase returns always a new database connection from CONNECTION_STRING.
func NewDatabase(applicationName string) *sql.DB {
	return newDatabase(
		context.Background(),
		fmt.Sprintf("host='%s' port='%d' user='%s' password='%s' dbname='%s' application_name='%s'",
			Hostname(), Port(), Username(), Password(), DatabaseName(), applicationName))
}

// NewDatabaseWithContext returns always a new database connection from CONNECTION_STRING.
//
// Context will be used only during connection check.
func NewDatabaseWithContext(ctx context.Context, applicationName string) *sql.DB {
	return newDatabase(
		ctx,
		fmt.Sprintf("host='%s' port='%d' user='%s' password='%s' dbname='%s' application_name='%s'",
			Hostname(), Port(), Username(), Password(), DatabaseName(), applicationName))
}

// NewInitDatabase returns always a new database connection from CONNECTION_STRING.
func NewInitDatabase(applicationName string) *sql.DB {
	return newDatabase(
		context.Background(),
		fmt.Sprintf("host='%s' port='%d' user='%s' password='%s' dbname='%s' application_name='%s'",
			InitHostname(), InitPort(), InitUsername(), InitPassword(), InitDatabaseName(), applicationName))
}

// NewInitDatabaseWithContext returns always a new database connection from CONNECTION_STRING.
//
// Context will be used only during connection check.
func NewInitDatabaseWithContext(ctx context.Context, applicationName string) *sql.DB {
	return newDatabase(
		ctx,
		fmt.Sprintf("host='%s' port='%d' user='%s' password='%s' dbname='%s' application_name='%s'",
			InitHostname(), InitPort(), InitUsername(), InitPassword(), InitDatabaseName(), applicationName))
}

// CloseDatabase closes the default database hold by this package.
func CloseDatabase() {
	if database != nil {
		_ = database.Close()
		database = nil
	}
}

// Listen waits for notifications on database channel and writes the payload to the go channel.
// The type of the go channel have to correspond to the payload JSON structure
func Listen[T any](conn *pgx.Conn, channel string, payloads chan T, errors chan error) {
	ListenWithContext(context.Background(), conn, channel, payloads, errors)
}

// ListenWithContext waits for notifications on database channel and writes the payload to the go channel.
// The type of the go channel have to correspond to the payload JSON structure
func ListenWithContext[T any](ctx context.Context, conn *pgx.Conn, channel string, payloads chan T, errors chan error) {
	rawPayloads := make(chan string)
	go ListenRawWithContext(ctx, conn, channel, rawPayloads, errors)

	for rawPayload := range rawPayloads {
		var payload T
		err := json.Unmarshal([]byte(rawPayload), &payload)
		if err != nil {
			log.Error("Database", "Unmarshal error during listening: %v", err)
			errors <- err
		} else {
			timeout := time.After(1 * time.Minute)
			select {
			case payloads <- payload:
			case <-timeout:
				log.Warn("websocket", "payloads channel full, producer is dropping messages")
			}
		}
	}
}

func ListenRawWithContext(ctx context.Context, conn *pgx.Conn, channel string, payloads chan string, errors chan error) {
	_, err := conn.Exec(ctx, "LISTEN "+channel)
	if err != nil {
		log.Error("Database", "Error listening on channel '%s': %v", channel, err)
		errors <- err
	}

	// Wait for notifications
	for {
		notification, err := conn.WaitForNotification(ctx)
		if pgconn.Timeout(err) {
			errors <- nil
		}
		if err != nil {
			log.Error("Database", "Error during listening for notifications: %v", err)
			errors <- err
			return
		}
		if notification != nil {
			var payload = notification.Payload
			payload = strings.TrimPrefix(payload, "~") // for deletion
			timeout := time.After(1 * time.Minute)
			select {
			case payloads <- payload:
			case <-timeout:
				log.Warn("websocket", "payloads channel full, producer is dropping messages")
			}
		}
	}
}

// Exec inserts a row using the given sql with arguments
func Exec(connection Connection, sql string, args ...interface{}) error {
	_, err := connection.Exec(context.Background(), sql, args...)
	if err != nil {
		log.Error("Database", "Error in statement '%s': %v", sql, err)
	}
	return err
}

func EmptyFloatIsNull(float *float64) pgtype.Float8 {
	return FloatIsNull(float, 0)
}

func FloatIsNull(float *float64, null float64) pgtype.Float8 {
	if float == nil || *float == null {
		return pgtype.Float8{Valid: false}
	}
	return pgtype.Float8{Float64: *float, Valid: true}
}

func EmptyStringIsNull[T any](string *T) pgtype.Text {
	return StringIsNull(string, "")
}

func StringIsNull[T any](s *T, null string) pgtype.Text {
	if s == nil || fmt.Sprintf("%v", *s) == null {
		return pgtype.Text{Valid: false}
	}
	return pgtype.Text{String: fmt.Sprintf("%v", *s), Valid: true}
}

func EmptyLongIntIsNull(int *int64) pgtype.Int8 {
	return LongIntIsNull(int, 0)
}

func LongIntIsNull(int *int64, null int64) pgtype.Int8 {
	if int == nil || *int == null {
		return pgtype.Int8{Valid: false}
	}
	return pgtype.Int8{Int64: *int, Valid: true}
}

func EmptyIntIsNull(int *int32) pgtype.Int4 {
	return IntIsNull(int, 0)
}

func IntIsNull(int *int32, null int32) pgtype.Int4 {
	if int == nil || *int == null {
		return pgtype.Int4{Valid: false}
	}
	return pgtype.Int4{Int32: *int, Valid: true}
}

func EmptySmallIntIsNull(int *int16) pgtype.Int2 {
	return SmallIntIsNull(int, 0)
}

func SmallIntIsNull(int *int16, null int16) pgtype.Int2 {
	if int == nil || *int == null {
		return pgtype.Int2{Valid: false}
	}
	return pgtype.Int2{Int16: *int, Valid: true}
}

// Begin returns a new transaction
func Begin(connection Connection) (pgx.Tx, error) {
	transaction, err := connection.Begin(context.Background())
	if err != nil {
		log.Error("Database", "Error starting transaction: %v", err)
		return transaction, err
	}
	return transaction, nil
}

// Query gets values read from database into a channel. The value type of channel must match
// the fields defined in the query. The type can be a single value (e.g. string) if the query
// returns only a single field. Otherwise, the type have to be a struct with the identical number
// of elements and corresponding types like the query statement
func Query[T any](connection Connection, sql string, results chan T, args ...interface{}) error {
	defer close(results)
	rows, err := connection.Query(context.Background(), sql, args...)
	if err != nil {
		log.Error("Database", "Error in query statement '%s': %v", sql, err)
		return err
	} else {
		defer rows.Close()
		for rows.Next() {
			var result T
			err := rows.Scan(interfaces(&result)...)
			if err != nil {
				log.Error("Database", "Error scanning result '%s': %v", sql, err)
				return err
			}
			timeout := time.After(1 * time.Minute)
			select {
			case results <- result:
			case <-timeout:
				log.Warn("websocket", "results channel full, producer is dropping messages")
			}
		}
	}
	return nil
}

// QuerySingleRow returns the value if only a single row is queried
func QuerySingleRow[T any](connection Connection, sql string, args ...interface{}) (T, error) {
	result := make(chan T)
	err := make(chan error)
	defer close(err)
	go func() {
		err <- Query(connection, sql, result, args...)
	}()
	return <-result, <-err
}

// interfaces creates interface for the given holder, or if the holder a structure this
// function returns a list of interfaces for all structure members.
func interfaces(holder interface{}) []interface{} {
	value := reflect.ValueOf(holder).Elem()
	if value.Kind() == reflect.Struct {
		values := make([]interface{}, value.NumField())
		for i := 0; i < value.NumField(); i++ {
			if value.Field(i).Kind() == reflect.Pointer {
				if value.Field(i).IsNil() {
					value.Field(i).Set(reflect.New(value.Field(i).Type().Elem()))
				}
				values[i] = value.Field(i).Interface()
			} else {
				values[i] = value.Field(i).Addr().Interface()
			}
		}
		return values
	} else {
		values := make([]interface{}, 1)
		values[0] = value.Addr().Interface()
		return values
	}
}
