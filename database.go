package doomdb

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/DoomLordor/logger"

	"github.com/lib/pq"
)

type dbHandle interface {
	Close() error
	BindNamed(query string, arg any) (string, []any, error)
	Select(dest interface{}, query string, args ...any) error
	Get(dest interface{}, query string, args ...any) error
	QueryRow(query string, args ...any) *sql.Row
	NamedExec(query string, arg any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
}

type DB struct {
	dbHandle   dbHandle
	logger     *logger.Logger
	cache      *cacheFieldsDB
	selectOnly bool
}

func NewDB(db dbHandle, config Config, logger *logger.Logger) *DB {
	return &DB{
		dbHandle:   db,
		logger:     logger,
		cache:      newCache(),
		selectOnly: config.SelectOnly,
	}
}

func (d *DB) Close() error {
	return d.dbHandle.Close()
}

func (d *DB) getSelectQuery(table, filters, order string, limit, offset uint64, dest any) string {
	queryRaw := make([]string, 0, 20)
	queryRaw = append(queryRaw, "SELECT", d.cache.getSelectFields(dest), "FROM", table, d.cache.getJoins(dest))
	if filters != "" {
		queryRaw = append(queryRaw, "WHERE")
		queryRaw = append(queryRaw, filters)
	}

	if order != "" {
		queryRaw = append(queryRaw, "ORDER BY")
		queryRaw = append(queryRaw, order)
	}

	if limit > 0 {
		queryRaw = append(queryRaw, "LIMIT")
		queryRaw = append(queryRaw, strconv.FormatUint(limit, 10))
	}

	if offset > 0 {
		queryRaw = append(queryRaw, "OFFSET")
		queryRaw = append(queryRaw, strconv.FormatUint(offset, 10))
	}

	return strings.Join(queryRaw, " ")
}

func (d *DB) getAll(table, filters, order string, limit, offset uint64, dest any, args ...any) error {
	query := d.getSelectQuery(table, filters, order, limit, offset, dest)

	d.logger.Debug().Str("table_name", table).Str("get_all", query).Send()
	return d.dbHandle.Select(dest, query, args...)
}

func (d *DB) GetAll(table, filters, order string, limit, offset uint64, dest any, args ...any) error {
	err := d.getAll(table, filters, order, limit, offset, dest, args...)
	if err != nil {
		d.logger.Err(err).Str("method", "get_all").Msg("")
		return SelectError
	}
	return nil
}

func (d *DB) getOne(table, filters, order string, limit, offset uint64, dest any, args ...any) error {
	query := d.getSelectQuery(table, filters, order, limit, offset, dest)

	d.logger.Debug().Str("table_name", table).Str("get_one", query).Send()
	return d.dbHandle.Get(dest, query, args...)
}

func (d *DB) GetOne(table, filters, order string, limit, offset uint64, dest any, args ...any) error {
	err := d.getOne(table, filters, order, limit, offset, dest, args...)
	if err != nil {
		d.logger.Err(err).Str("method", "get_one").Msg("")
		return SelectError
	}
	return nil
}

func (d *DB) create(table string, dest any) (uint64, error) {
	fields, values := d.cache.getInsertFields(dest)
	queryRaw := []string{
		"INSERT INTO",
		table,
		fmt.Sprintf("(%s)", fields),
		"VALUES",
		fmt.Sprintf("(%s)", values),
		"RETURNING id",
	}
	query := strings.Join(queryRaw, " ")
	d.logger.Debug().Str("table_name", table).Str("create", query).Send()
	queryNamed, args, err := d.dbHandle.BindNamed(query, dest)
	if err != nil {
		return 0, err
	}
	id := 0
	err = d.dbHandle.QueryRow(queryNamed, args...).Scan(&id)

	if err != nil {
		return 0, err
	}
	return uint64(id), nil
}

func (d *DB) Create(table string, dest any) (uint64, error) {
	if d.selectOnly {
		return rand.Uint64(), nil
	}
	id, err := d.create(table, dest)
	if err != nil {
		d.logger.Err(err).Send()
		return 0, CreateError
	}
	return id, nil
}

func (d *DB) update(table string, dest any, id uint64) (sql.Result, error) {
	fields := d.cache.getUpdateFields(dest)
	queryRaw := []string{
		"UPDATE",
		table,
		"SET",
		fields,
		fmt.Sprintf(" WHERE id=%d", id),
	}
	query := strings.Join(queryRaw, " ")
	d.logger.Debug().Str("table_name", table).Str("update", query).Send()
	return d.dbHandle.NamedExec(query, dest)
}

func (d *DB) Update(table string, arg any, id uint64) (sql.Result, error) {
	if d.selectOnly {
		return nil, nil
	}
	res, err := d.update(table, arg, id)
	if err != nil {
		d.logger.Err(err).Str("method", "update").Msg("")
		return nil, UpdateError
	}
	return res, nil
}

func (d *DB) query(query string, arg ...any) error {
	_, err := d.dbHandle.Query(query, arg...)
	if err != nil {
		return err
	}
	return nil
}

func (d *DB) Query(query string, arg ...any) error {
	if d.selectOnly {
		return nil
	}
	d.logger.Debug().Str("query", query).Send()
	err := d.query(query, arg...)
	if err != nil {
		d.logger.Err(err).Send()
		return QueryError
	}
	return nil
}

func (d *DB) Delete(table, fieldName string, arg any) error {
	if d.selectOnly {
		return nil
	}
	queryRaw := []string{
		"DELETE FROM",
		table,
		fmt.Sprintf(`WHERE "%s" = $1`, fieldName),
	}
	query := strings.Join(queryRaw, " ")
	d.logger.Debug().Str("delete", query).Send()
	err := d.query(query, arg)
	if err != nil {
		d.logger.Err(err).Send()
		return DeleteError
	}
	return nil
}

func (d *DB) Array(a any) interface {
	driver.Valuer
	sql.Scanner
} {
	return pq.Array(a)
}
