package gosql

import (
	"context"
	"database/sql"
	"log/slog"
)

type txKey struct{}

// RO represents read-only transaction options
var (
	RO = &sql.TxOptions{ReadOnly: true}
	// RW represents read-write transaction options
	RW = &sql.TxOptions{ReadOnly: false}

	// TxKey is the context key used to store and retrieve transaction objects
	TxKey = txKey{}
)

// Page represents a paginated result set of items
type Page[T any] struct {
	Items      []T `json:"items" yaml:"items"`
	TotalPages int `json:"totalPages" yaml:"totalPages"`
}

// Paging represents pagination parameters
type Paging struct {
	PageNum  int `json:"pageNum" yaml:"pageNum"`
	PageSize int `json:"pageSize" yaml:"pageSize"`
}

// Normalize ensures that pagination parameters have valid values
func (p *Paging) Normalize() {
	if p.PageNum <= 0 || p.PageSize <= 0 {
		p.PageNum = 1
		p.PageSize = 20
	}
}

// GetOffset calculates the offset for SQL queries based on page number and size
func (p Paging) GetOffset() int {
	return (p.PageNum - 1) * p.PageSize
}

// GetLimit returns the page size as a limit for SQL queries
func (p Paging) GetLimit() int {
	return p.PageSize
}

// GetTotalPages calculates the total number of pages based on total rows and page size
func (p Paging) GetTotalPages(totalRows int) int {
	result := totalRows / p.PageSize
	if totalRows%p.PageSize > 0 {
		result++
	}
	return result
}

// Exec executes a SQL statement with the given arguments
func Exec(ctx context.Context, tx *sql.Tx, stmt *sql.Stmt, args ...any) error {
	slog.DebugContext(ctx, "Executing SQL statement", "stmt", stmt, "args_count", len(args))
	_, err := tx.StmtContext(ctx, stmt).ExecContext(ctx, args...)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to execute SQL statement", "error", err)
	}
	return err
}

// Query executes a SQL query and returns a slice of results
func Query[T any](ctx context.Context, tx *sql.Tx, stmt *sql.Stmt, newReceiver func() T, dstFields func(T) []any, args ...any) ([]T, error) {
	slog.DebugContext(ctx, "Executing SQL query", "stmt", stmt, "args_count", len(args))
	rows, err := tx.StmtContext(ctx, stmt).QueryContext(ctx, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			slog.DebugContext(ctx, "No rows returned from query")
			return nil, nil
		}
		slog.ErrorContext(ctx, "Failed to execute SQL query", "error", err)
		return nil, err
	}

	defer rows.Close()

	res := make([]T, 0)
	for rows.Next() {
		t := newReceiver()
		if err := rows.Scan(dstFields(t)...); err != nil {
			slog.ErrorContext(ctx, "Failed to scan row", "error", err)
			return nil, err
		}
		res = append(res, t)
	}
	slog.DebugContext(ctx, "Query returned results", "count", len(res))
	return res, nil
}

// QueryOne executes a SQL query and returns a single result
func QueryOne[T any](ctx context.Context, tx *sql.Tx, stmt *sql.Stmt, newReceiver func() T, dstFields func(T) []any, args ...any) (T, error) {
	slog.DebugContext(ctx, "Executing SQL query for single result", "stmt", stmt, "args_count", len(args))
	row := tx.StmtContext(ctx, stmt).QueryRowContext(ctx, args...)

	t := newReceiver()
	if err := row.Scan(dstFields(t)...); err != nil {
		if err == sql.ErrNoRows {
			slog.DebugContext(ctx, "No row found for query")
			return Nil[T](), err
		}
		slog.ErrorContext(ctx, "Failed to scan row", "error", err)
		return t, err
	}
	slog.DebugContext(ctx, "Query returned single result")
	return t, nil
}

// QueryPage executes a SQL query with pagination and returns a Page of results
func QueryPage[T any](ctx context.Context, tx *sql.Tx, countStmt, stmt *sql.Stmt, paging Paging, newReceiver func() T, dstFields func(T) []any, args ...any) (Page[T], error) {
	slog.DebugContext(ctx, "Executing paginated SQL query", "paging", paging)
	count, err := QueryVal[int](ctx, tx, countStmt, args...)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to get count for paginated query", "error", err)
		return Page[T]{}, err
	}

	paging.Normalize()
	items, err := Query(ctx, tx, stmt, newReceiver, dstFields, append(args, paging.GetLimit(), paging.GetOffset())...)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to get items for paginated query", "error", err)
		return Page[T]{}, err
	}

	result := Page[T]{Items: items, TotalPages: paging.GetTotalPages(count)}
	slog.DebugContext(ctx, "Paginated query completed", "total_items", count, "returned_items", len(items), "total_pages", result.TotalPages)
	return result, nil
}

// QueryVal executes a SQL query and returns a single scalar value
func QueryVal[T any](ctx context.Context, tx *sql.Tx, stmt *sql.Stmt, args ...any) (T, error) {
	slog.DebugContext(ctx, "Executing SQL query for scalar value", "stmt", stmt, "args_count", len(args))
	row := tx.StmtContext(ctx, stmt).QueryRowContext(ctx, args...)

	var t T
	if err := row.Scan(&t); err != nil {
		slog.ErrorContext(ctx, "Failed to scan scalar value", "error", err)
		return t, err
	}
	slog.DebugContext(ctx, "Query returned scalar value")
	return t, nil
}

// ExecWithTx executes an operation within a transaction
// If a transaction already exists in the context, it will be reused
func ExecWithTx(ctx context.Context, db *sql.DB, opts *sql.TxOptions, operation func(context.Context, *sql.Tx) error) error {
	tx := ctx.Value(TxKey)
	if tx == nil {
		slog.DebugContext(ctx, "Starting new transaction", "read_only", opts.ReadOnly)
		tx, err := db.BeginTx(ctx, opts)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to begin transaction", "error", err)
			return err
		}
		defer tx.Rollback()

		ctx = context.WithValue(ctx, TxKey, tx)

		if err := operation(ctx, tx); err != nil {
			slog.ErrorContext(ctx, "Operation failed within transaction", "error", err)
			return err
		}

		slog.DebugContext(ctx, "Committing transaction")
		return tx.Commit()
	} else {
		slog.DebugContext(ctx, "Reusing existing transaction from context")
		ctx = context.WithValue(ctx, TxKey, tx)
		if err := operation(ctx, tx.(*sql.Tx)); err != nil {
			slog.ErrorContext(ctx, "Operation failed within transaction", "error", err)
			return err
		}
		return nil
	}
}

// QueryWithTx executes an operation that returns a result within a transaction
// If a transaction already exists in the context, it will be reused
func QueryWithTx[T any](ctx context.Context, db *sql.DB, opts *sql.TxOptions, operation func(context.Context, *sql.Tx) (T, error)) (T, error) {
	tx := ctx.Value(TxKey)
	if tx == nil {
		slog.DebugContext(ctx, "Starting new transaction for query", "read_only", opts.ReadOnly)
		tx, err := db.BeginTx(ctx, opts)
		if err != nil {
			var t T
			slog.ErrorContext(ctx, "Failed to begin transaction for query", "error", err)
			return t, err
		}
		defer tx.Rollback()

		ctx = context.WithValue(ctx, TxKey, tx)

		res, err := operation(ctx, tx)
		if err != nil {
			slog.ErrorContext(ctx, "Query operation failed within transaction", "error", err)
			return res, err
		}

		slog.DebugContext(ctx, "Committing transaction after query")
		if err = tx.Commit(); err != nil {
			slog.ErrorContext(ctx, "Failed to commit transaction after query", "error", err)
			return res, err
		}

		return res, nil
	} else {
		slog.DebugContext(ctx, "Reusing existing transaction from context for query")
		ctx = context.WithValue(ctx, TxKey, tx)

		res, err := operation(ctx, tx.(*sql.Tx))
		if err != nil {
			slog.ErrorContext(ctx, "Query operation failed within transaction", "error", err)
			return res, err
		}
		return res, nil
	}
}
