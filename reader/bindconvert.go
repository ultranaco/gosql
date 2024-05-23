package reader

import (
	"log"
	"strconv"
	"time"

	"github.com/ultranaco/gosql"
)

// DefineColumns define columns and bind with converter
func DefineColumns(row gosql.RowMapper, columns ...string) (map[string]*BindConvert, error) {
	columnBinders := map[string]*BindConvert{}
	binders := make([]interface{}, len(columns))

	for index, column := range columns {
		binder := &[]byte{}
		columnBinders[column] = &BindConvert{RawData: binder}
		binders[index] = binder
	}

	err := row.Scan(binders...)

	return columnBinders, err
}

// BindConvert bind convert
type BindConvert struct {
	RawData *[]byte
}

// GetString convert a byte array to string of chars
func (b *BindConvert) GetString() string {
	if b.RawData == nil || len(*b.RawData) <= 0 {
		return ""
	}
	return string(*b.RawData)
}

// GetInt convert a byte array to integer
func (b *BindConvert) GetInt64() int {
	if b.RawData == nil || len(*b.RawData) <= 0 {
		return 0
	}
	rawNumber := string(*b.RawData)
	num, err := strconv.Atoi(rawNumber)

	if err != nil {
		log.Println(err)
		return 0
	}

	return num
}

// GetInt convert a byte array to integer
func (b *BindConvert) GetInt() int64 {
	if b.RawData == nil || len(*b.RawData) <= 0 {
		return 0
	}
	rawNumber := string(*b.RawData)
	num, err := strconv.ParseInt(rawNumber, 10, 64)

	if err != nil {
		log.Println(err)
		return 0
	}

	return num
}

// GetFloat32 convert a byte array to float32
func (b *BindConvert) GetFloat32() float32 {
	if b.RawData == nil || len(*b.RawData) <= 0 {
		return 0
	}
	rawNumber := string(*b.RawData)
	num, err := strconv.ParseFloat(rawNumber, 32)

	if err != nil {
		log.Println(err)
		return 0
	}

	return float32(num)
}

// GetFloat64 convert a byte array to float64
func (b *BindConvert) GetFloat64() float64 {
	if b.RawData == nil || len(*b.RawData) <= 0 {
		return 0
	}
	rawNumber := string(*b.RawData)
	num, err := strconv.ParseFloat(rawNumber, 64)

	if err != nil {
		log.Println(err)
		return 0
	}

	return float64(num)
}

// GetDateTime convert byte array to Time RFC3339
func (b *BindConvert) GetDateTime() time.Time {
	if b.RawData == nil || len(*b.RawData) <= 0 {
		return time.Time{}
	}
	rawTime := string(*b.RawData)
	date, err := time.Parse(time.RFC3339, rawTime)

	if err != nil {
		log.Println(err)
		return time.Time{}
	}

	return date
}

// GetDateTimeCompat convert byte array to Time RFC3339 with backward compatibility
func (b *BindConvert) GetDateTimeCompat(layerCompat string) time.Time {
	if b.RawData == nil || len(*b.RawData) <= 0 {
		return time.Time{}
	}

	rawTime := string(*b.RawData)
	date, err := time.Parse(time.RFC3339, rawTime)

	if err != nil {
		date, err := time.Parse(layerCompat, rawTime)
		if err != nil {
			log.Printf("ERROR %s", err)
		}
		return date
	}

	return date
}

// GetBool convert byte array to bool
func (b *BindConvert) GetBool() bool {
	if b.RawData == nil || len(*b.RawData) <= 0 {
		return false
	}
	rawBool := string(*b.RawData)
	if rawBool == "true" {
		return true
	}
	return false
}
