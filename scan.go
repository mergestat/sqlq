package sqlq

import (
	"database/sql"
	"reflect"
)

// scanJob() is a utility method that scans the result of a query (from *sql.Rows) into the provided *Job.
// It overrides existing values in the job object wherever a corresponding column is found in the query.
// Any other field is left untouched.
func scanJob(rows *sql.Rows, job *Job) (err error) {
	defer func() { _ = rows.Close() }()

	var columns []string
	if columns, err = rows.Columns(); err != nil {
		return err
	}

	// resolve the named field using the "db" tag
	var fieldByName = func(v reflect.Value, name string) reflect.Value {
		for i := 0; i < v.NumField(); i++ {
			tag, ok := v.Type().Field(i).Tag.Lookup("db")
			if ok && tag == name {
				return v.Field(i)
			}
		}

		return reflect.ValueOf(nil)
	}

	// use if over for-loop because we only support scanning a single value
	if rows.Next() {
		var rv = reflect.ValueOf(job).Elem() // de-reference pointer and get a handle of the underlying job object

		// array of pointers "pointing" to the respective fields in the job object
		var pointers = make([]interface{}, len(columns))

		// for all columns in the query, resolve their field type and get a pointer to the memory location
		for i, col := range columns {
			var fv = fieldByName(rv, col)
			if !fv.IsValid() || !fv.CanSet() {
				var nothing interface{}
				pointers[i] = &nothing
			} else {
				pointers[i] = fv.Addr().Interface()
			}
		}

		// scan results using the array of pointers
		if err = rows.Scan(pointers...); err != nil {
			return err
		}
	} else {
		return sql.ErrNoRows
	}

	return rows.Err()
}
