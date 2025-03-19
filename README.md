# GoSQL - A Type-Safe SQL Library for Go

GoSQL is a lightweight, type-safe SQL library for Go that provides a clean abstraction over the standard `database/sql` package. It offers a robust Data Access Object (DAO) pattern implementation with support for transactions, pagination, and entity relationship management.

## Features

- **Type-safe queries** with generics support
- **DAO pattern** implementation for entity management
- **Transaction management** with context-based propagation
- **Pagination** support for large result sets
- **Entity versioning** to prevent concurrent modification issues
- **Cascading operations** for entity relationships
- **Prepared statement caching** for improved performance
- **Comprehensive logging** with structured logs via `slog`

## Installation

```bash
go get github.com/iglin/go-sql
```

## Core Concepts

### Entity Interface

The library revolves around the `Entity` interface, which defines the contract for database entities:

```go
type Entity interface {
	comparable
	GetID() uuid.UUID
	SetID(uuid.UUID)
	GetVersion() uuid.UUID
	SetVersion(uuid.UUID)
	Equals(another any) bool
}
```

A base implementation, `GenericEntity`, is provided for convenience:

```go
type GenericEntity struct {
	ID      uuid.UUID `json:"id" yaml:"id"`
	Version uuid.UUID `json:"version" yaml:"version"`
}
```

### Data Access Objects (DAOs)

The `Dao` interface provides CRUD operations for entities:

```go
type Dao[T Entity] interface {
	Save(ctx context.Context, entities ...T) error
	FindById(ctx context.Context, id uuid.UUID) (T, error)
	FindOneByStmt(ctx context.Context, stmt *QueryOneStmt[T], args ...any) (T, error)
	ListByStmt(ctx context.Context, stmt *QueryStmt[T], args ...any) ([]T, error)
	ListAll(ctx context.Context) ([]T, error)
	ListPageByStmt(ctx context.Context, stmt *QueryPageStmt[T], paging Paging, args ...any) (Page[T], error)
	ListPage(ctx context.Context, paging Paging) (Page[T], error)
	Delete(ctx context.Context, entities ...T) error
	DeleteCascade(ctx context.Context, entities ...T) error
	DeleteByIds(ctx context.Context, ids ...uuid.UUID) error
	DeleteByIdsCascade(ctx context.Context, ids ...uuid.UUID) error
	Close(ctx context.Context) error
}
```

### Statements

GoSQL provides several statement types for different query operations:

- `ExecStmt`: For executing commands without returning rows
- `QueryValStmt<T>`: For retrieving a single scalar value
- `QueryOneStmt<T>`: For retrieving a single entity
- `QueryStmt<T>`: For retrieving multiple entities
- `QueryPageStmt<T>`: For retrieving paginated results

### Pagination

The library includes built-in pagination support:

```go
type Page[T any] struct {
	Items      []T `json:"items" yaml:"items"`
	TotalPages int `json:"totalPages" yaml:"totalPages"`
}

type Paging struct {
	PageNum  int `json:"pageNum" yaml:"pageNum"`
	PageSize int `json:"pageSize" yaml:"pageSize"`
}
```

### Transaction Management

Transactions are managed through context propagation:

```go
// Transaction options
var (
	RO = &sql.TxOptions{ReadOnly: true}
	RW = &sql.TxOptions{ReadOnly: false}
	TxKey = txKey{}
)
```

## Usage Examples

### Creating a DAO

```go
// Define your entity
type User struct {
	gosql.GenericEntity
	Name  string
	Email string
}

// Create a DAO for the User entity
userDao := gosql.NewDaoBuilder[User]{
	DB:              db,
	InsertStmt:      &gosql.ExecStmt{BaseStmt: gosql.BaseStmt{Query: "INSERT INTO users (id, version, name, email) VALUES (?, ?, ?, ?)", Cache: true}},
	UpdateStmt:      &gosql.ExecStmt{BaseStmt: gosql.BaseStmt{Query: "UPDATE users SET version = ?, name = ?, email = ? WHERE id = ? AND version = ?", Cache: true}},
	GetByIdStmt:     createGetByIdStmt(),
	ListAllStmt:     createListAllStmt(),
	ListAllPageStmt: createListAllPageStmt(),
	DeleteByIdStmt:  &gosql.ExecStmt{BaseStmt: gosql.BaseStmt{Query: "DELETE FROM users WHERE id = ?", Cache: true}},
	InsertArgs:      createInsertArgsFunc(),
	UpdateArgs:      createUpdateArgsFunc(),
	SaveChildren:    func(ctx context.Context, tx *sql.Tx, e User) error { return nil },
	LoadChildren:    func(ctx context.Context, tx *sql.Tx, e User) error { return nil },
	DeleteChildren:  func(ctx context.Context, tx *sql.Tx, e User) error { return nil },
}.Build()
```

### Saving Entities

```go
user := User{
	Name:  "John Doe",
	Email: "john@example.com",
}

// Save a new user
err := userDao.Save(ctx, user)
```

### Finding Entities

```go
// Find by ID
user, err := userDao.FindById(ctx, userId)

// Custom query
stmt := &gosql.QueryOneStmt[User]{
	BaseStmt: gosql.BaseStmt{
		Query: "SELECT id, version, name, email FROM users WHERE email = ?",
		Cache: true,
	},
	NewReceiver: func() User { return User{} },
	Receive: func(u User) []any {
		return []any{&u.ID, &u.Version, &u.Name, &u.Email}
	},
}
user, err := userDao.FindOneByStmt(ctx, stmt, "john@example.com")
```

### Listing Entities

```go
// List all users
users, err := userDao.ListAll(ctx)

// Paginated list
paging := gosql.Paging{PageNum: 1, PageSize: 10}
page, err := userDao.ListPage(ctx, paging)
```

### Deleting Entities

```go
// Delete by entity
err := userDao.Delete(ctx, user)

// Delete by ID
err := userDao.DeleteByIds(ctx, userId)

// Delete with cascade
err := userDao.DeleteCascade(ctx, user)
```

### Working with Transactions

```go
// Execute in a transaction
result, err := gosql.QueryWithTx(ctx, db, gosql.RW, func(ctx context.Context, tx *sql.Tx) (Result, error) {
    // Perform multiple operations in a transaction
    err := userDao.Save(ctx, user)
    if err != nil {
        return nil, err
    }
    
    // More operations...
    
    return result, nil
})
```

## Best Practices

1. **Use context propagation** for transaction management
2. **Enable statement caching** for frequently used queries
3. **Implement proper entity versioning** to prevent concurrent modification issues
4. **Define appropriate relationship handlers** for cascading operations
5. **Use pagination** for large result sets

## Error Handling

The library provides predefined errors:

```go
var (
	ErrNotFound = errors.New("gosql: entity not found")
	ErrVersionMismatch = errors.New("gosql: version mismatch - entity was modified")
)
```

## Closing Resources

Always close DAOs when they're no longer needed:

```go
err := userDao.Close(ctx)
```

