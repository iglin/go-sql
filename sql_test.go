package gosql

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestPaging(t *testing.T) {
	tests := []struct {
		name          string
		paging        Paging
		expectedPage  int
		expectedSize  int
		expectedTotal int
		totalRows     int
	}{
		{
			name:          "Zero values are normalized",
			paging:        Paging{PageNum: 0, PageSize: 0},
			expectedPage:  1,
			expectedSize:  20,
			totalRows:     45,
			expectedTotal: 3,
		},
		{
			name:          "Negative values are normalized",
			paging:        Paging{PageNum: -1, PageSize: -5},
			expectedPage:  1,
			expectedSize:  20,
			totalRows:     45,
			expectedTotal: 3,
		},
		{
			name:          "Valid values remain unchanged",
			paging:        Paging{PageNum: 2, PageSize: 10},
			expectedPage:  2,
			expectedSize:  10,
			totalRows:     25,
			expectedTotal: 3,
		},
		{
			name:          "Exact division",
			paging:        Paging{PageNum: 1, PageSize: 5},
			expectedPage:  1,
			expectedSize:  5,
			totalRows:     15,
			expectedTotal: 3,
		},
		{
			name:          "With remainder",
			paging:        Paging{PageNum: 1, PageSize: 10},
			expectedPage:  1,
			expectedSize:  10,
			totalRows:     22,
			expectedTotal: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.paging.Normalize()
			if tt.paging.PageNum != tt.expectedPage {
				t.Errorf("Expected page number %d, got %d", tt.expectedPage, tt.paging.PageNum)
			}
			if tt.paging.PageSize != tt.expectedSize {
				t.Errorf("Expected page size %d, got %d", tt.expectedSize, tt.paging.PageSize)
			}
			if tt.paging.GetOffset() != (tt.paging.PageNum-1)*tt.paging.PageSize {
				t.Errorf("Incorrect offset calculation")
			}
			if tt.paging.GetLimit() != tt.paging.PageSize {
				t.Errorf("Incorrect limit calculation")
			}
			totalPages := tt.paging.GetTotalPages(tt.totalRows)
			if totalPages != tt.expectedTotal {
				t.Errorf("Expected total pages %d, got %d", tt.expectedTotal, totalPages)
			}
		})
	}
}

func TestExecWithTxNested(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test nested transactions
	ctx := context.Background()
	err = ExecWithTx(ctx, db, RW, func(ctx context.Context, tx *sql.Tx) error {
		// First insert
		stmt, err := tx.PrepareContext(ctx, "INSERT INTO test (value) VALUES (?)")
		if err != nil {
			return err
		}
		defer stmt.Close()

		_, err = stmt.ExecContext(ctx, "first")
		if err != nil {
			return err
		}

		// Nested transaction should use the same tx
		return ExecWithTx(ctx, db, RW, func(ctx context.Context, tx *sql.Tx) error {
			stmt, err := tx.PrepareContext(ctx, "INSERT INTO test (value) VALUES (?)")
			if err != nil {
				return err
			}
			defer stmt.Close()
			_, err = stmt.ExecContext(ctx, "second")
			return err
		})
	})

	if err != nil {
		t.Fatalf("Failed to execute nested transactions: %v", err)
	}

	// Verify both inserts were successful
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

func TestQueryWithTxNested(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table and insert test data
	_, err = db.Exec(`
		CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);
		INSERT INTO test (value) VALUES ('test');
	`)
	if err != nil {
		t.Fatalf("Failed to setup test data: %v", err)
	}

	// Test nested query transactions
	ctx := context.Background()
	result, err := QueryWithTx(ctx, db, RO, func(ctx context.Context, tx *sql.Tx) (string, error) {
		// First query
		stmt, err := tx.PrepareContext(ctx, "SELECT value FROM test WHERE id = 1")
		if err != nil {
			return "", err
		}
		defer stmt.Close()

		var value string
		row := stmt.QueryRowContext(ctx)
		if err := row.Scan(&value); err != nil {
			return "", err
		}

		// Nested query should use the same tx
		nestedValue, err := QueryWithTx(ctx, db, RO, func(ctx context.Context, tx *sql.Tx) (string, error) {
			stmt, err := tx.PrepareContext(ctx, "SELECT value FROM test WHERE id = 1")
			if err != nil {
				return "", err
			}
			defer stmt.Close()

			var v string
			row := stmt.QueryRowContext(ctx)
			if err := row.Scan(&v); err != nil {
				return "", err
			}
			return v, nil
		})

		if err != nil {
			return "", err
		}

		if value != nestedValue {
			t.Errorf("Values don't match: %s != %s", value, nestedValue)
		}

		return value, nil
	})

	if err != nil {
		t.Fatalf("Failed to execute nested queries: %v", err)
	}

	if result != "test" {
		t.Errorf("Expected result 'test', got '%s'", result)
	}
}

func TestQueryValNoRows(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, RO)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, "SELECT value FROM test WHERE id = ?")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	_, err = QueryVal[string](ctx, tx, stmt, 1)
	if err != sql.ErrNoRows {
		t.Errorf("Expected sql.ErrNoRows, got %v", err)
	}
}

func TestQueryOneNoRows(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, RO)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	type TestStruct struct {
		ID    int
		Value string
	}

	stmt, err := tx.PrepareContext(ctx, "SELECT id, value FROM test WHERE id = ?")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	result, err := QueryOne(ctx, tx, stmt,
		func() *TestStruct { return &TestStruct{} },
		func(t *TestStruct) []any { return []any{&t.ID, &t.Value} },
		1)

	if err != sql.ErrNoRows {
		t.Errorf("Expected sql.ErrNoRows, got %v", err)
	}

	if result != nil {
		t.Errorf("Expected nil result, got %v", result)
	}
}

func TestQueryNoRows(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, RO)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	type TestStruct struct {
		ID    int
		Value string
	}

	stmt, err := tx.PrepareContext(ctx, "SELECT id, value FROM test WHERE id > ?")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	results, err := Query(ctx, tx, stmt,
		func() *TestStruct { return &TestStruct{} },
		func(t *TestStruct) []any { return []any{&t.ID, &t.Value} },
		100)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if results == nil || len(results) != 0 {
		t.Errorf("Expected empty slice, got %v", results)
	}
}

func TestQueryPageError(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, RO)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Prepare statements
	countStmt, err := tx.PrepareContext(ctx, "SELECT COUNT(*) FROM test")
	if err != nil {
		t.Fatalf("Failed to prepare count statement: %v", err)
	}
	defer countStmt.Close()

	listStmt, err := tx.PrepareContext(ctx, "SELECT id, value FROM test LIMIT ? OFFSET ?")
	if err != nil {
		t.Fatalf("Failed to prepare list statement: %v", err)
	}
	defer listStmt.Close()

	type TestStruct struct {
		ID    int
		Value string
	}

	page, err := QueryPage(ctx, tx, countStmt, listStmt,
		Paging{PageNum: 1, PageSize: 10},
		func() *TestStruct { return &TestStruct{} },
		func(t *TestStruct) []any { return []any{&t.ID, &t.Value} })

	if err != nil {
		t.Errorf("Expected no error for empty table, got %v", err)
	}

	if page.TotalPages != 0 || len(page.Items) != 0 {
		t.Error("Expected empty page for empty table")
	}

	// Test with invalid statement
	invalidStmt, err := tx.PrepareContext(ctx, "SELECT COUNT(*) FROM nonexistent_table")
	if err == nil {
		defer invalidStmt.Close()
		_, err = QueryPage(ctx, tx, invalidStmt, listStmt,
			Paging{PageNum: 1, PageSize: 10},
			func() *TestStruct { return &TestStruct{} },
			func(t *TestStruct) []any { return []any{&t.ID, &t.Value} })
		if err == nil {
			t.Error("Expected error for invalid count statement")
		}
	}
}

func TestNilHelpers(t *testing.T) {
	type TestStruct struct {
		Value string
	}

	// Test Nil function
	nilValue := Nil[TestStruct]()
	if nilValue.Value != "" {
		t.Error("Expected empty struct from Nil function")
	}

	// Test IsNil function
	if !IsNil(nilValue) {
		t.Error("Expected IsNil to return true for zero value")
	}

	nonNilValue := TestStruct{Value: "test"}
	if IsNil(nonNilValue) {
		t.Error("Expected IsNil to return false for non-zero value")
	}
}
