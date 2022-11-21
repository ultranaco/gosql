package gosql

import (
	gosql "database/sql"
	"errors"
	"log"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"
)

var (
	clients map[string]Client
	lock    sync.RWMutex
)

func init() {
	clients = make(map[string]Client)
}

// Init set or get a cliente sql connection instance and prevents ephemeral ports exhaustion
func Init(connectionKey, connectionString, driverName string, connectionPoolSize int) (Client, error) {
	var client Client
	var err error

	client = GetClient(connectionKey)

	if client.ClientKey == "" {
		lock.Lock()
		client = clients[connectionKey]
		// Note: double check is necesary instead of sync.Once connectionString can change
		if client.ClientKey == "" {
			client, err = makeConnection(connectionKey, connectionString, driverName, connectionPoolSize)
			if err != nil {
				return Client{}, err
			}
			clients[connectionKey] = client
		}
		lock.Unlock()
	}

	return client, err
}

// GetClient retrieve client
func GetClient(connectionKey string) Client {
	var client Client

	lock.RLock()
	client = clients[connectionKey]
	lock.RUnlock()

	return client
}

// makeConnection Create a connection
func makeConnection(connectionKey, connString, driverName string, connectionPoolSize int) (Client, error) {

	var connTime int
	connlife := os.Getenv("SQL_MAX_CONN_LIFE")

	if connlife == "" {
		connTime = int(2 * time.Minute)
	} else {
		connTime, _ = strconv.Atoi(connlife)
	}

	db, err := gosql.Open(driverName, connString)
	if err == nil {
		db.SetMaxIdleConns(connectionPoolSize / 3)
		db.SetMaxOpenConns(connectionPoolSize)
		db.SetConnMaxLifetime(time.Duration(connTime) * time.Millisecond)
	} else {
		log.Println("func makeConnection => ", err)
	}

	if db == nil {
		return Client{}, errors.New("failed to open a database connection gg")
	}

	err = db.Ping()
	if err != nil {
		return Client{}, err
	}

	return Client{
		Client:    db,
		ClientKey: connectionKey,
	}, nil
}

// QueryRow Obtains only one result of the command
func QueryRow(connectionKey, sqlCommand string, scanner func(rm RowMapper) (interface{}, error), args ...interface{}) (interface{}, error) {
	client := GetClient(connectionKey)
	return client.QueryRow(sqlCommand, scanner, args...)
}

// Query Obtains only one result of the command
func Query(connectionKey, sqlCommand string, items interface{}, scanner func(rm RowMapper) (interface{}, error), args ...interface{}) error {
	client := GetClient(connectionKey)
	return client.Query(sqlCommand, items, scanner, args...)
}

// Exec Execute a command and return affected rows
func Exec(connectionKey, sqlCommand string, args ...interface{}) (int64, error) {
	client := GetClient(connectionKey)
	return client.Exec(sqlCommand, args...)
}

// Client struct
type Client struct {
	Client    *gosql.DB
	ClientKey string
}

// QueryRow Obtains only one result of the command
func (p *Client) QueryRow(sqlCommand string, scanner func(rm RowMapper) (interface{}, error), args ...interface{}) (interface{}, error) {
	queryrow := p.Client.QueryRow(sqlCommand, args...) // Only first row
	item, err := scanner(queryrow)
	return item, err
}

// Query Obtains results of the command
func (p *Client) Query(sqlCommand string, items interface{}, scanner func(rm RowMapper) (interface{}, error), args ...interface{}) error {
	rows, err := p.Client.Query(sqlCommand, args...)

	if err != nil {
		return err
	}

	defer rows.Close()

	elements := reflect.ValueOf(items).Elem()

	for rows.Next() {
		item, err := scanner(rows)

		if err != nil {
			return err
		}

		elements.Set(reflect.Append(elements, reflect.ValueOf(item)))
	}

	return nil
}

// Exec Execute a command and return affected rows
func (p *Client) Exec(sqlCommand string, args ...interface{}) (int64, error) {
	result, err := p.Client.Exec(sqlCommand, args...)

	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}
