package csvq

import (
	"database/sql/driver"
	"database/sql"
	"fmt"
	"time"
)

type ValueConverter struct {
}

func (c ValueConverter) ConvertValue(v interface{}) (driver.Value, error) {
	if IsCsvqValue(v) {
		return v, nil
	}

	if v == nil {
		return Null{}, nil
	}

	switch v.(type) {
	case string:
		return String{value: v.(string)}, nil
	case int:
		return Integer{value: int64(v.(int))}, nil
	case int8:
		return Integer{value: int64(v.(int8))}, nil
	case int16:
		return Integer{value: int64(v.(int16))}, nil
	case int32:
		return Integer{value: int64(v.(int32))}, nil
	case int64:
		return Integer{value: v.(int64)}, nil
	case uint:
		return Integer{value: int64(v.(uint))}, nil
	case uint8:
		return Integer{value: int64(v.(uint8))}, nil
	case uint16:
		return Integer{value: int64(v.(uint16))}, nil
	case uint32:
		return Integer{value: int64(v.(uint32))}, nil
	case uint64:
		u64 := v.(uint64)
		if u64 >= 1<<63 {
			return nil, fmt.Errorf("uint64 values with high bit set are not supported")
		}
		return Integer{value: int64(u64)}, nil
	case float32:
		return Float{value: float64(v.(float32))}, nil
	case float64:
		return Float{value: v.(float64)}, nil
	case bool:
		return Boolean{value: v.(bool)}, nil
	case time.Time:
		return Datetime{value: v.(time.Time)}, nil
	case *sql.NullInt16:
		val := v.(*sql.NullInt16)
		if val.Valid {
			return Integer{value: int64(val.Int16)}, nil
		}
		return Null{}, nil
	case *sql.NullInt32:
		val := v.(*sql.NullInt32)
		if val.Valid {
			return Integer{value: int64(val.Int32)}, nil
		}
		return Null{}, nil
	case *sql.NullInt64:
		val := v.(*sql.NullInt64)
		if val.Valid {
			return Integer{value: val.Int64}, nil
		}
		return Null{}, nil
	case *sql.NullFloat64:
		val := v.(*sql.NullFloat64)
		if val.Valid {
			return Float{value: val.Float64}, nil
		}
		return Null{}, nil
	case *sql.NullBool:
		val := v.(*sql.NullBool)
		if val.Valid {
			return Boolean{value: val.Bool}, nil
		}
		return Null{}, nil
	case *sql.NullString:
		val := v.(*sql.NullString)
		if val.Valid {
			return String{value: val.String}, nil
		}
		return Null{}, nil
	case *sql.NullTime:
		val := v.(*sql.NullTime)
		if val.Valid {
			return Datetime{value: val.Time}, nil
		}
		return Null{}, nil
	case *sql.NullByte:
		val := v.(*sql.NullByte)
		if val.Valid {
			return String{value: string([]byte{val.Byte})}, nil
		}
		return Null{}, nil
	}

	return nil, fmt.Errorf("unsupported type: %T", v)
}

func IsCsvqValue(v interface{}) bool {
	switch v.(type) {
	case String, Integer, Float, Boolean, Datetime, Null:
		return true
	}
	return false
}
