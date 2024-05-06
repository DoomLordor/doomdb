package doomdb

import (
	"database/sql"
)

type dbHandleTest struct {
}

func (d dbHandleTest) Close() error {
	return nil
}

func (d dbHandleTest) Rebind(query string) string {
	return query
}

func (d dbHandleTest) BindNamed(query string, arg any) (string, []any, error) {
	return "", []any{}, nil
}

func (d dbHandleTest) Select(dest interface{}, query string, args ...any) error {
	return nil
}

func (d dbHandleTest) QueryRow(query string, args ...any) *sql.Row {
	return nil
}

func (d dbHandleTest) NamedExec(query string, arg any) (sql.Result, error) {
	return nil, nil
}

func (d dbHandleTest) Query(query string, args ...any) (*sql.Rows, error) {
	return nil, nil
}
