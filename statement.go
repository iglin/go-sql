package gosql

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
)

// BaseStmt represents the base structure for all statement types
type BaseStmt struct {
	Query      string
	Cache      bool
	cachedStmt *sql.Stmt
}

// ExecStmt represents a statement that executes a command without returning rows
type ExecStmt struct {
	BaseStmt
}

// QueryValStmt represents a statement that returns a single scalar value
type QueryValStmt[T any] struct {
	BaseStmt
}

// QueryOneStmt represents a statement that returns a single entity
type QueryOneStmt[T any] struct {
	BaseStmt
	NewReceiver func() T
	Receive     func(T) []any
}

// QueryStmt represents a statement that returns multiple entities
type QueryStmt[T any] struct {
	BaseStmt
	NewReceiver func() T
	Receive     func(T) []any
}

// QueryPageStmt represents a statement that returns a paginated result set
type QueryPageStmt[T any] struct {
	CountStmt *QueryValStmt[int]
	QueryStmt *QueryStmt[T]
}

// prepare prepares a statement for execution, using a cached version if available
func (stmt *BaseStmt) prepare(ctx context.Context, tx *sql.Tx) (*sql.Stmt, error) {
	if stmt.Cache && stmt.cachedStmt != nil {
		return stmt.cachedStmt, nil
	}
	var err error
	var stmtToUse *sql.Stmt
	stmtToUse, err = tx.PrepareContext(ctx, stmt.Query)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to prepare cached statement", "query", stmt.Query, "error", err)
		return nil, err
	}
	if stmt.Cache {
		stmt.cachedStmt = stmtToUse
	}
	return stmtToUse, nil
}

// Exec executes a gosql statement with the given arguments
func (stmt *ExecStmt) Exec(ctx context.Context, tx *sql.Tx, args ...any) error {
	slog.DebugContext(ctx, "Executing gosql statement", "stmt", stmt.Query, "cache", stmt.Cache)
	stmtToUse, err := stmt.prepare(ctx, tx)
	if err != nil {
		return err
	}

	if !stmt.Cache {
		defer stmtToUse.Close()
	}

	return Exec(ctx, tx, stmtToUse, args...)
}

// Close releases resources associated with the statement
func (stmt *BaseStmt) Close(ctx context.Context) error {
	slog.DebugContext(ctx, "Closing cached statement", "stmt", stmt.Query)
	if stmt.cachedStmt != nil {
		err := stmt.cachedStmt.Close()
		if err != nil {
			slog.ErrorContext(ctx, "Failed to close cached statement", "error", err)
			return err
		}
	}
	return nil
}

// Query executes a SQL query and returns a single scalar value
func (stmt *QueryValStmt[T]) Query(ctx context.Context, tx *sql.Tx, args ...any) (T, error) {
	slog.DebugContext(ctx, "Executing gosql query for scalar value", "stmt", stmt.Query, "args_count", len(args))
	stmtToUse, err := stmt.prepare(ctx, tx)
	if err != nil {
		return Nil[T](), err
	}

	if !stmt.Cache {
		defer stmtToUse.Close()
	}

	return QueryVal[T](ctx, tx, stmtToUse, args...)
}

// Query executes a SQL query and returns multiple entities
func (stmt *QueryStmt[T]) Query(ctx context.Context, tx *sql.Tx, args ...any) ([]T, error) {
	slog.DebugContext(ctx, "Executing gosql query", "stmt", stmt.Query, "args_count", len(args))
	stmtToUse, err := stmt.prepare(ctx, tx)
	if err != nil {
		return nil, err
	}

	if !stmt.Cache {
		defer stmtToUse.Close()
	}

	return Query(ctx, tx, stmtToUse, stmt.NewReceiver, stmt.Receive, args...)
}

// Query executes a SQL query and returns a single entity
func (stmt *QueryOneStmt[T]) Query(ctx context.Context, tx *sql.Tx, args ...any) (T, error) {
	slog.DebugContext(ctx, "Executing gosql query", "stmt", stmt.Query, "args_count", len(args))
	stmtToUse, err := stmt.prepare(ctx, tx)
	if err != nil {
		return Nil[T](), err
	}

	if !stmt.Cache {
		defer stmtToUse.Close()
	}

	return QueryOne(ctx, tx, stmtToUse, stmt.NewReceiver, stmt.Receive, args...)
}

// QueryPage executes a gosql query with pagination and returns a Page of results
func (stmt *QueryPageStmt[T]) QueryPage(ctx context.Context, tx *sql.Tx, paging Paging, args ...any) (Page[T], error) {
	slog.DebugContext(ctx, "Executing gosql query with pagination", "stmt", stmt.QueryStmt.Query, "args_count", len(args), "paging", paging)
	countStmt, err := stmt.CountStmt.prepare(ctx, tx)
	if err != nil {
		return Page[T]{}, err
	}
	queryStmt, err := stmt.QueryStmt.prepare(ctx, tx)
	if err != nil {
		return Page[T]{}, err
	}

	if !stmt.CountStmt.Cache {
		defer countStmt.Close()
	}
	if !stmt.QueryStmt.Cache {
		defer queryStmt.Close()
	}

	return QueryPage[T](ctx, tx, countStmt, queryStmt, paging, stmt.QueryStmt.NewReceiver, stmt.QueryStmt.Receive, args...)
}

// Close releases resources associated with the paginated query statement
func (stmt *QueryPageStmt[T]) Close(ctx context.Context) error {
	slog.DebugContext(ctx, "Closing paginated query statement")
	errs := make([]error, 0, 2)
	if err := stmt.CountStmt.Close(ctx); err != nil {
		slog.ErrorContext(ctx, "Failed to close count statement", "error", err)
		errs = append(errs, err)
	}
	if err := stmt.QueryStmt.Close(ctx); err != nil {
		slog.ErrorContext(ctx, "Failed to close query statement", "error", err)
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
