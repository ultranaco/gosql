package gosql

// RowMapper interface
type RowMapper interface {
	Scan(dest ...interface{}) error
}
