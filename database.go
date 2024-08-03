package doomdb

import (
	"context"
	"database/sql"
	"math/rand"

	"github.com/DoomLordor/logger"

	"github.com/Masterminds/squirrel"
)

type dbHandle interface {
	Close() error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...any) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...any) error
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
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

func (d *DB) queryBuilder() squirrel.StatementBuilderType {
	return squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
}

func (d *DB) getSelectQuery(table string, orders []string, limit, offset uint64, dest, filters any, args ...any) squirrel.SelectBuilder {

	query := d.queryBuilder().Select(d.cache.getSelectFields(dest)...).From(table)

	if filters != nil {
		query = query.Where(filters, args...)
	}

	for _, join := range d.cache.getJoins(dest) {
		query = query.JoinClause(join)
	}

	query = query.OrderBy(orders...)

	if limit > 0 {
		query = query.Limit(limit).Offset(offset)
	}

	if offset > 0 {
		query = query.Offset(offset)
	}

	return query
}

func (d *DB) getAll(ctx context.Context, table string, orders []string, limit, offset uint64, dest, filters any, args ...any) error {
	query := d.getSelectQuery(table, orders, limit, offset, dest, filters, args)
	sqlQuery, args, err := query.ToSql()
	if err != nil {
		return err
	}

	d.logger.Debug().Str("table_name", table).Str("table_name", table).Str("get_all", sqlQuery).Send()
	return d.dbHandle.SelectContext(ctx, dest, sqlQuery, args...)
}

func (d *DB) GetAll(ctx context.Context, table string, orders []string, limit, offset uint64, dest, filters any, args ...any) error {
	err := d.getAll(ctx, table, orders, limit, offset, dest, filters, args...)
	if err != nil {
		d.logger.Err(err).Str("table_name", table).Str("method", "get_all").Msg("")
		return SelectError
	}

	return nil
}

func (d *DB) getOne(ctx context.Context, table string, dest, filters any, args ...any) error {
	query := d.getSelectQuery(table, nil, 0, 0, dest, filters, args...)
	sqlQuery, args, err := query.ToSql()
	if err != nil {
		return err
	}

	d.logger.Debug().Str("table_name", table).Str("get_one", sqlQuery).Send()
	return d.dbHandle.GetContext(ctx, dest, sqlQuery, args...)
}

func (d *DB) GetOne(ctx context.Context, table string, dest, filters any, args ...any) error {
	err := d.getOne(ctx, table, dest, filters, args...)
	if err != nil {
		d.logger.Err(err).Str("table_name", table).Str("method", "get_one").Msg("")
		return SelectError
	}

	return nil
}

func (d *DB) create(ctx context.Context, table string, dest any, withID bool) (uint64, error) {

	fields, values := d.cache.getInsertFields(dest)
	query := d.queryBuilder().Insert(table).Columns(fields...).Values(values...)

	if withID {
		query = query.Suffix("RETURNING id")
	}

	querySql, args, err := query.ToSql()
	if err != nil {
		return 0, err
	}
	d.logger.Debug().Str("table_name", table).Str("create", querySql).Send()

	if !withID {
		_, err = d.dbHandle.ExecContext(ctx, querySql, args...)
		return 0, err
	}

	var id uint64
	err = d.dbHandle.QueryRowContext(ctx, querySql, args...).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

func (d *DB) Create(ctx context.Context, table string, dest any, withID bool) (uint64, error) {
	if d.selectOnly {
		return rand.Uint64(), nil
	}
	id, err := d.create(ctx, table, dest, withID)
	if err != nil {
		d.logger.Err(err).Str("table_name", table).Send()
		return 0, CreateError
	}

	return id, nil
}

func (d *DB) update(ctx context.Context, table, fieldName string, value, dest any) (sql.Result, error) {
	fields := d.cache.get(dest, true)
	query := d.queryBuilder().Update(table).Where(squirrel.Eq{fieldName: value})

	for _, field := range fields {
		query = query.Set(field.name, field.value)
	}

	querySql, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}

	d.logger.Debug().Str("table_name", table).Str("update", querySql).Send()
	return d.dbHandle.ExecContext(ctx, querySql, args...)
}

func (d *DB) Update(ctx context.Context, table, fieldName string, value, dest any) (sql.Result, error) {
	if d.selectOnly {
		return nil, nil
	}
	res, err := d.update(ctx, table, fieldName, value, dest)
	if err != nil {
		d.logger.Err(err).Str("table_name", table).Str("method", "update").Send()
		return nil, UpdateError
	}

	return res, nil
}

func (d *DB) query(ctx context.Context, query string, arg ...any) error {
	_, err := d.dbHandle.QueryContext(ctx, query, arg...)
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) Query(ctx context.Context, query string, arg ...any) error {
	if d.selectOnly {
		return nil
	}
	d.logger.Debug().Str("query", query).Send()
	err := d.query(ctx, query, arg...)
	if err != nil {
		d.logger.Err(err).Send()
		return QueryError
	}

	return nil
}

func (d *DB) delete(ctx context.Context, table, fieldName string, arg any) error {
	if d.selectOnly {
		return nil
	}

	query := d.queryBuilder().Delete(table).Where(fieldName, arg)
	querySql, args, err := query.ToSql()
	if err != nil {
		return err
	}

	d.logger.Debug().Str("table_name", table).Str("delete", querySql).Send()

	return d.query(ctx, querySql, args...)
}

func (d *DB) Delete(ctx context.Context, table, fieldName string, arg any) error {
	err := d.delete(ctx, table, fieldName, arg)
	if err != nil {
		d.logger.Err(err).Str("table", table).Send()
		return DeleteError
	}

	return nil
}
