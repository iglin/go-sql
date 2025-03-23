package gosql

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"

	"github.com/google/uuid"
)

var (
	// ErrNotFound is returned when an entity cannot be found
	ErrNotFound = errors.New("gosql: entity not found")
	// ErrVersionMismatch is returned when an entity's version doesn't match the expected version
	ErrVersionMismatch = errors.New("gosql: version mismatch - entity was modified")
)

// Entity defines the interface for database entities that can be managed by the DAO
type Entity interface {
	comparable
	GetID() uuid.UUID
	SetID(uuid.UUID)
	GetVersion() uuid.UUID
	SetVersion(uuid.UUID)
	Equals(another any) bool
}

// Dao defines the interface for data access objects that manage entities
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

// GenericEntity is a base implementation of the Entity interface
type GenericEntity struct {
	ID      uuid.UUID `json:"id" yaml:"id"`
	Version uuid.UUID `json:"version" yaml:"version"`
}

// GetID returns the entity's ID
func (e *GenericEntity) GetID() uuid.UUID {
	return e.ID
}

// SetID sets the entity's ID
func (e *GenericEntity) SetID(id uuid.UUID) {
	e.ID = id
}

// GetVersion returns the entity's version
func (e *GenericEntity) GetVersion() uuid.UUID {
	return e.Version
}

// SetVersion sets the entity's version
func (e *GenericEntity) SetVersion(version uuid.UUID) {
	e.Version = version
}

// genericDao is a generic implementation of the Dao interface
type genericDao[T Entity] struct {
	db              *sql.DB
	insertStmt      *ExecStmt
	updateStmt      *ExecStmt
	getByIdStmt     *QueryOneStmt[T]
	listAllStmt     *QueryStmt[T]
	listAllPageStmt *QueryPageStmt[T]
	deleteByIdStmt  *ExecStmt

	insertArgs     func(T) []any
	updateArgs     func(T) []any
	saveChildren   func(ctx context.Context, tx *sql.Tx, e T) error
	loadChildren   func(ctx context.Context, tx *sql.Tx, e T) error
	deleteChildren func(ctx context.Context, tx *sql.Tx, e T) error
}

// DaoBuilder builds new Dao[T] object with the provided parameters. All of the parameters are mandatory.
type DaoBuilder[T Entity] struct {
	//DB: SQL database connection to use for all operations
	DB *sql.DB
	//InsertStmt: Statement for inserting new entities
	InsertStmt *DaoExecStmt
	//UpdateStmt: Statement for updating existing entities
	UpdateStmt *DaoExecStmt
	//GetByIdStmt: Statement for retrieving a single entity by ID
	GetByIdStmt *DaoQueryOneStmt[T]
	//ListAllStmt: Statement for retrieving all entities
	ListAllStmt *DaoQueryStmt[T]
	//ListAllPageStmt: Statement for retrieving paginated results of all entities
	ListAllPageStmt *DaoQueryPageStmt[T]
	//DeleteByIdStmt: Statement for deleting entity by its ID
	DeleteByIdStmt *DaoExecStmt
	//NewReceiver: Function that returns a new instance of the entity
	NewReceiver func() T
	//Receive: Function that returns the arguments for the update statement for a given entity
	Receive func(T) []any
	//InsertArgs: Function that returns the arguments for the insert statement for a given entity
	InsertArgs func(T) []any
	//UpdateArgs: Function that returns the arguments for the update statement for a given entity
	UpdateArgs func(T) []any
	//SaveChildren: Function that saves child entities associated with the parent entity
	SaveChildren func(ctx context.Context, tx *sql.Tx, e T) error
	//LoadChildren: Function that loads child entities associated with the parent entity
	LoadChildren func(ctx context.Context, tx *sql.Tx, e T) error
	//DeleteChildren: Function that deletes child entities associated with the parent entity
	DeleteChildren func(ctx context.Context, tx *sql.Tx, e T) error
}

func (b DaoBuilder[T]) Build(ctx context.Context) (Dao[T], error) {
	if err := b.validate(ctx); err != nil {
		return nil, err
	}
	return &genericDao[T]{
		db:              b.DB,
		insertStmt:      b.InsertStmt.ToStmt(),
		updateStmt:      b.UpdateStmt.ToStmt(),
		getByIdStmt:     b.GetByIdStmt.ToStmt(b.NewReceiver, b.Receive),
		listAllStmt:     b.ListAllStmt.ToStmt(b.NewReceiver, b.Receive),
		listAllPageStmt: b.ListAllPageStmt.ToStmt(b.NewReceiver, b.Receive),
		deleteByIdStmt:  b.DeleteByIdStmt.ToStmt(),
		insertArgs:      b.InsertArgs,
		updateArgs:      b.UpdateArgs,
		saveChildren:    b.SaveChildren,
		loadChildren:    b.LoadChildren,
		deleteChildren:  b.DeleteChildren,
	}, nil
}

func (b DaoBuilder[T]) validate(ctx context.Context) error {
	if b.DB == nil {
		slog.ErrorContext(ctx, "db is nil")
		return errors.New("gosql: db is nil")
	}
	if b.InsertStmt == nil {
		slog.ErrorContext(ctx, "insertStmt is nil")
		return errors.New("gosql: insertStmt is nil")
	}
	if b.InsertStmt.Query == "" {
		slog.ErrorContext(ctx, "insertStmt.Query is empty")
		return errors.New("gosql: insertStmt.Query is empty")
	}
	if b.UpdateStmt == nil {
		slog.ErrorContext(ctx, "udateStmt is nil")
		return errors.New("gosql: updateStmt is nil")
	}
	if b.UpdateStmt.Query == "" {
		slog.ErrorContext(ctx, "updateStmt.Query is empty")
		return errors.New("gosql: updateStmt.Query is empty")
	}
	if b.GetByIdStmt == nil {
		slog.ErrorContext(ctx, "getByIdStmt is nil")
		return errors.New("gosql: getByIdStmt is nil")
	}
	if b.GetByIdStmt.Query == "" {
		slog.ErrorContext(ctx, "getByIdStmt.Query is empty")
		return errors.New("gosql: getByIdStmt.Query is empty")
	}
	if b.ListAllStmt == nil {
		slog.ErrorContext(ctx, "listAllStmt is nil")
		return errors.New("gosql: listAllStmt is nil")
	}
	if b.ListAllPageStmt == nil {
		slog.ErrorContext(ctx, "listAllPageStmt is nil")
		return errors.New("gosql: listAllPageStmt is nil")
	}
	if b.ListAllPageStmt.CountStmt == nil {
		slog.ErrorContext(ctx, "listAllPageStmt.CountStmt is nil")
		return errors.New("gosql: listAllPageStmt.CountStmt is nil")
	}
	if b.ListAllPageStmt.QueryStmt == nil {
		slog.ErrorContext(ctx, "listAllPageStmt.QueryStmt is nil")
		return errors.New("gosql: listAllPageStmt.QueryStmt is nil")
	}
	if b.DeleteByIdStmt == nil {
		slog.ErrorContext(ctx, "deleteByIdStmt is nil")
		return errors.New("gosql: deleteByIdStmt is nil")
	}
	if b.DeleteByIdStmt.Query == "" {
		slog.ErrorContext(ctx, "deleteByIdStmt.Query is empty")
		return errors.New("gosql: deleteByIdStmt.Query is empty")
	}
	if b.NewReceiver == nil {
		slog.ErrorContext(ctx, "newReceiver is nil")
		return errors.New("gosql: newReceiver is nil")
	}
	if b.Receive == nil {
		slog.ErrorContext(ctx, "receive is nil")
		return errors.New("gosql: receive is nil")
	}
	if b.InsertArgs == nil {
		slog.ErrorContext(ctx, "insertArgs is nil")
		return errors.New("gosql: insertArgs is nil")
	}
	if b.UpdateArgs == nil {
		slog.ErrorContext(ctx, "updateArgs is nil")
		return errors.New("gosql: updateArgs is nil")
	}
	if b.SaveChildren == nil {
		slog.ErrorContext(ctx, "saveChildren is nil")
		return errors.New("gosql: saveChildren is nil")
	}
	if b.LoadChildren == nil {
		slog.ErrorContext(ctx, "loadChildren is nil")
		return errors.New("gosql: loadChildren is nil")
	}
	if b.DeleteChildren == nil {
		slog.ErrorContext(ctx, "deleteChildren is nil")
		return errors.New("gosql: deleteChildren is nil")
	}
	return nil
}

// Save persists an entity to the database
func (dao *genericDao[T]) Save(ctx context.Context, e ...T) error {
	slog.DebugContext(ctx, "Saving entities", "entities_count", len(e))
	if len(e) == 0 {
		return nil
	}
	return ExecWithTx(ctx, dao.db, RW, func(ctx context.Context, tx *sql.Tx) error {
		for _, entity := range e {
			if err := dao.save(ctx, tx, entity); err != nil {
				return err
			}
		}
		return nil
	})
}

func (dao *genericDao[T]) save(ctx context.Context, tx *sql.Tx, e T) error {
	slog.DebugContext(ctx, "Saving entity", "id", e.GetID())
	if e.GetID() == uuid.Nil {
		e.SetID(uuid.New())
		e.SetVersion(uuid.New())
		slog.DebugContext(ctx, "Inserting new entity", "id", e.GetID())

		if err := dao.insertStmt.Exec(ctx, tx, dao.insertArgs(e)...); err != nil {
			slog.ErrorContext(ctx, "Failed to insert entity", "id", e.GetID(), "error", err)
			return err
		}
	} else {
		slog.DebugContext(ctx, "Updating existing entity", "id", e.GetID())
		existing, err := dao.findById(ctx, tx, e.GetID())
		if err != nil {
			slog.ErrorContext(ctx, "Failed to find existing entity for update", "id", e.GetID(), "error", err)
			return err
		}
		if IsNil(existing) {
			slog.ErrorContext(ctx, "Entity not found for update", "id", e.GetID())
			return ErrNotFound
		}

		if e.Equals(existing) {
			slog.DebugContext(ctx, "Entity unchanged, skipping update", "id", e.GetID())
			return nil
		}

		if e.GetVersion() != existing.GetVersion() {
			slog.ErrorContext(ctx, "Version mismatch during update", "id", e.GetID(), "expected", existing.GetVersion(), "actual", e.GetVersion())
			return ErrVersionMismatch
		}
		e.SetVersion(uuid.New())

		if err := dao.updateStmt.Exec(ctx, tx, dao.updateArgs(e)...); err != nil {
			slog.ErrorContext(ctx, "Failed to update entity", "id", e.GetID(), "error", err)
			return err
		}
	}

	slog.DebugContext(ctx, "Saving entity children", "id", e.GetID())
	return dao.saveChildren(ctx, tx, e)
}

// FindById retrieves an entity by its ID
func (dao *genericDao[T]) FindById(ctx context.Context, id uuid.UUID) (T, error) {
	slog.DebugContext(ctx, "Finding entity by ID", "id", id)
	return QueryWithTx(ctx, dao.db, RO, func(ctx context.Context, tx *sql.Tx) (T, error) {
		return dao.findById(ctx, tx, id)
	})
}

func (dao *genericDao[T]) findById(ctx context.Context, tx *sql.Tx, id uuid.UUID) (T, error) {
	res, err := dao.getByIdStmt.Query(ctx, tx, id)
	if err != nil {
		if err == sql.ErrNoRows {
			slog.DebugContext(ctx, "Entity not found by ID", "id", id)
		} else {
			slog.ErrorContext(ctx, "Error finding entity by ID", "id", id, "error", err)
		}
		return res, err
	}
	slog.DebugContext(ctx, "Loading entity children", "id", id)
	if err := dao.loadChildren(ctx, tx, res); err != nil {
		slog.ErrorContext(ctx, "Error loading entity children", "id", id, "error", err)
		return res, err
	}
	return res, nil
}

// FindOneByStmt retrieves a single entity using a custom SQL statement
func (dao *genericDao[T]) FindOneByStmt(ctx context.Context, stmt *QueryOneStmt[T], args ...any) (T, error) {
	slog.DebugContext(ctx, "Finding one entity by statement", "args_count", len(args))
	return QueryWithTx(ctx, dao.db, RO, func(ctx context.Context, tx *sql.Tx) (T, error) {
		res, err := stmt.Query(ctx, tx, args...)
		if err != nil {
			slog.ErrorContext(ctx, "Error finding entity by statement", "error", err)
			return res, err
		}

		slog.DebugContext(ctx, "Loading entity children", "id", res.GetID())
		if err := dao.loadChildren(ctx, tx, res); err != nil {
			slog.ErrorContext(ctx, "Error loading entity children", "id", res.GetID(), "error", err)
			return res, err
		}
		return res, nil
	})
}

// ListByStmt retrieves entities using a custom SQL statement
func (dao *genericDao[T]) ListByStmt(ctx context.Context, stmt *QueryStmt[T], args ...any) ([]T, error) {
	slog.DebugContext(ctx, "Listing entities by statement", "args_count", len(args))
	return QueryWithTx(ctx, dao.db, RO, func(ctx context.Context, tx *sql.Tx) ([]T, error) {
		res, err := stmt.Query(ctx, tx, args...)
		if err != nil {
			slog.ErrorContext(ctx, "Error listing entities by statement", "error", err)
			return nil, err
		}
		slog.DebugContext(ctx, "Loading children for entities", "count", len(res))
		for _, e := range res {
			item := e
			if err := dao.loadChildren(ctx, tx, item); err != nil {
				slog.ErrorContext(ctx, "Error loading entity children", "id", item.GetID(), "error", err)
				return nil, err
			}
		}
		return res, nil
	})
}

// ListAll retrieves all entities
func (dao *genericDao[T]) ListAll(ctx context.Context) ([]T, error) {
	slog.DebugContext(ctx, "Listing all entities")
	return QueryWithTx(ctx, dao.db, RO, func(ctx context.Context, tx *sql.Tx) ([]T, error) {
		res, err := dao.listAllStmt.Query(ctx, tx)
		if err != nil {
			slog.ErrorContext(ctx, "Error listing all entities", "error", err)
			return nil, err
		}
		slog.DebugContext(ctx, "Loading children for all entities", "count", len(res))
		for _, e := range res {
			item := e
			if err := dao.loadChildren(ctx, tx, item); err != nil {
				slog.ErrorContext(ctx, "Error loading entity children", "id", item.GetID(), "error", err)
				return nil, err
			}
		}
		return res, nil
	})
}

// ListPageByStmt retrieves a paginated list of entities using a custom SQL statement
func (dao *genericDao[T]) ListPageByStmt(ctx context.Context, stmt *QueryPageStmt[T], paging Paging, args ...any) (Page[T], error) {
	slog.DebugContext(ctx, "Listing page of entities by statement", "paging", paging, "args_count", len(args))
	return QueryWithTx(ctx, dao.db, RO, func(ctx context.Context, tx *sql.Tx) (Page[T], error) {
		res, err := stmt.QueryPage(ctx, tx, paging, args...)
		if err != nil {
			slog.ErrorContext(ctx, "Error listing page of entities by statement", "error", err)
			return Page[T]{}, err
		}
		slog.DebugContext(ctx, "Loading children for page of entities", "count", len(res.Items))
		for _, e := range res.Items {
			item := e
			if err := dao.loadChildren(ctx, tx, item); err != nil {
				slog.ErrorContext(ctx, "Error loading entity children", "id", item.GetID(), "error", err)
				return Page[T]{}, err
			}
		}
		return res, nil
	})
}

// ListPage retrieves a paginated list of all entities
func (dao *genericDao[T]) ListPage(ctx context.Context, paging Paging) (Page[T], error) {
	slog.DebugContext(ctx, "Listing page of all entities", "paging", paging)
	return QueryWithTx(ctx, dao.db, RO, func(ctx context.Context, tx *sql.Tx) (Page[T], error) {
		res, err := dao.listAllPageStmt.QueryPage(ctx, tx, paging)
		if err != nil {
			slog.ErrorContext(ctx, "Error listing page of all entities", "error", err)
			return Page[T]{}, err
		}
		slog.DebugContext(ctx, "Loading children for page of all entities", "count", len(res.Items))
		for _, e := range res.Items {
			item := e
			if err := dao.loadChildren(ctx, tx, item); err != nil {
				slog.ErrorContext(ctx, "Error loading entity children", "id", item.GetID(), "error", err)
				return Page[T]{}, err
			}
		}
		return res, nil
	})
}

// Delete removes entities from the database
func (dao *genericDao[T]) Delete(ctx context.Context, entities ...T) error {
	slog.DebugContext(ctx, "Deleting entities", "count", len(entities))
	if len(entities) == 0 {
		return nil
	}

	return ExecWithTx(ctx, dao.db, RW, func(ctx context.Context, tx *sql.Tx) error {
		for _, e := range entities {
			entity := e
			slog.DebugContext(ctx, "Deleting entity by id", "id", entity.GetID())
			if err := dao.deleteByIdStmt.Exec(ctx, tx, entity.GetID()); err != nil {
				slog.ErrorContext(ctx, "Error deleting entity", "id", entity.GetID(), "error", err)
				return err
			}
		}
		return nil
	})
}

// DeleteCascade removes entities and their children from the database
func (dao *genericDao[T]) DeleteCascade(ctx context.Context, entities ...T) error {
	slog.DebugContext(ctx, "Deleting entities with cascade", "count", len(entities))
	if len(entities) == 0 {
		return nil
	}
	return ExecWithTx(ctx, dao.db, RW, func(ctx context.Context, tx *sql.Tx) error {
		return dao.deleteCascade(ctx, tx, entities...)
	})
}

func (dao *genericDao[T]) deleteCascade(ctx context.Context, tx *sql.Tx, entities ...T) error {
	slog.DebugContext(ctx, "Deleting entities after children", "count", len(entities))
	if len(entities) == 0 {
		return nil
	}
	for _, e := range entities {
		entity := e
		slog.DebugContext(ctx, "Deleting entity children", "id", entity.GetID())
		if err := dao.deleteChildren(ctx, tx, entity); err != nil {
			slog.ErrorContext(ctx, "Error deleting entity children", "id", entity.GetID(), "error", err)
			return err
		}
		if err := dao.deleteByIdStmt.Exec(ctx, tx, entity.GetID()); err != nil {
			slog.ErrorContext(ctx, "Error deleting entity", "id", entity.GetID(), "error", err)
			return err
		}
	}
	return nil
}

// DeleteByIds removes entities by their IDs
func (dao *genericDao[T]) DeleteByIds(ctx context.Context, ids ...uuid.UUID) error {
	slog.DebugContext(ctx, "Deleting entities by IDs", "count", len(ids))
	if len(ids) == 0 {
		return nil
	}
	return ExecWithTx(ctx, dao.db, RW, func(ctx context.Context, tx *sql.Tx) error {
		for _, id := range ids {
			if err := dao.deleteByIdStmt.Exec(ctx, tx, id); err != nil {
				slog.ErrorContext(ctx, "Error deleting entity", "id", id, "error", err)
				return err
			}
		}
		return nil
	})
}

// DeleteByIdsCascade removes entities and their children by the entities' IDs
func (dao *genericDao[T]) DeleteByIdsCascade(ctx context.Context, ids ...uuid.UUID) error {
	slog.DebugContext(ctx, "Deleting entities by IDs with cascade", "count", len(ids))
	if len(ids) == 0 {
		return nil
	}
	return ExecWithTx(ctx, dao.db, RW, func(ctx context.Context, tx *sql.Tx) error {
		entities := make([]T, 0, len(ids))
		for _, id := range ids {
			entity, err := dao.FindById(ctx, id)
			if err != nil {
				slog.ErrorContext(ctx, "Error listing entities for cascade delete", "error", err)
				return err
			}
			if !IsNil(entity) {
				entities = append(entities, entity)
			}
		}
		return dao.deleteCascade(ctx, tx, entities...)
	})
}

// Close closes all prepared statements in the DAO
// This should be called when the DAO is no longer needed to free up resources
func (dao *genericDao[T]) Close(ctx context.Context) error {
	slog.DebugContext(ctx, "Closing GenericDao prepared statements")
	errs := make([]error, 0)
	if err := dao.insertStmt.Close(ctx); err != nil {
		slog.Error("Failed to close insert statement", "error", err)
		errs = append(errs, err)
	}
	if err := dao.updateStmt.Close(ctx); err != nil {
		slog.ErrorContext(ctx, "Failed to close update statement", "error", err)
		errs = append(errs, err)
	}
	if err := dao.getByIdStmt.Close(ctx); err != nil {
		slog.ErrorContext(ctx, "Failed to close getById statement", "error", err)
		errs = append(errs, err)
	}
	if err := dao.listAllStmt.Close(ctx); err != nil {
		slog.ErrorContext(ctx, "Failed to close listAll statement", "error", err)
		errs = append(errs, err)
	}
	if err := dao.listAllPageStmt.Close(ctx); err != nil {
		slog.ErrorContext(ctx, "Failed to close listAllPage statement", "error", err)
		errs = append(errs, err)
	}
	if err := dao.deleteByIdStmt.Close(ctx); err != nil {
		slog.ErrorContext(ctx, "Failed to close deleteByIds statement", "error", err)
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	slog.DebugContext(ctx, "Successfully closed all GenericDao prepared statements")
	return nil
}

// ToSliceOfAny converts a slice of values to a slice of interface{} values
func ToSliceOfAny[T any](slice ...T) []any {
	res := make([]any, 0, len(slice))
	for _, e := range slice {
		res = append(res, e)
	}
	return res
}

// Nil returns a zero value of type T
func Nil[T any]() T {
	var _nil T
	return _nil
}

// IsNil checks if a value is the zero value of its type
func IsNil[T comparable](v T) bool {
	var _nil T
	return v == _nil
}
