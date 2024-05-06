package doomdb

import "errors"

var (
	SelectError = errors.New("db handle select error")
	CreateError = errors.New("db handle create error")
	UpdateError = errors.New("db handle update error")
	QueryError  = errors.New("db handle query error")
	DeleteError = errors.New("db handle delete error")
)
