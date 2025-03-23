package gosql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

var (
	ctx = context.Background()
)

type Student struct {
	GenericEntity
	Name       string
	Department *Department
}

func (s *Student) Equals(another any) bool {
	if another == nil {
		return false
	}
	if s == another {
		return true
	}
	anotherStudent, ok := another.(*Student)
	return ok && s.Name == anotherStudent.Name && s.Department.Equals(anotherStudent.Department)
}

type Department struct {
	GenericEntity
	Name string
}

func (d *Department) Equals(another any) bool {
	if another == nil {
		return false
	}
	if another == d {
		return true
	}
	anotherDpt, ok := another.(*Department)
	return ok && d.Name == anotherDpt.Name
}

func initDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite3 database: %v", err)
	}

	// Create departments table
	_, err = db.Exec(`
	    BEGIN;

		CREATE TABLE departments (
			id TEXT PRIMARY KEY,
			version TEXT NOT NULL,
			name TEXT NOT NULL
		);

		CREATE TABLE students (
			id TEXT PRIMARY KEY,
			version TEXT NOT NULL,
			name TEXT NOT NULL,
			department_id TEXT NOT NULL,
			FOREIGN KEY (department_id) REFERENCES departments(id)
		);

		COMMIT;
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	return db
}

func newDepartmentDao(t *testing.T, db *sql.DB) Dao[*Department] {
	// SQL statements for Department operations
	const (
		insertSQL      = `INSERT INTO departments (id, name, version) VALUES (?, ?, ?)`
		updateSQL      = `UPDATE departments SET name = ?, version = ? WHERE id = ?`
		getByIDSQL     = `SELECT id, name, version FROM departments WHERE id = ?`
		listAllSQL     = `SELECT id, name, version FROM departments`
		countAllSQL    = `SELECT COUNT(*) FROM departments`
		listAllPageSQL = `SELECT id, name, version FROM departments ORDER BY name LIMIT ? OFFSET ?`
		deleteByIDSQL  = `DELETE FROM departments WHERE id = ?`
	)

	// Create DAO instance
	newReceiver := func() *Department { return &Department{} }
	receive := func(d *Department) []any { return []any{&d.ID, &d.Name, &d.Version} }
	departmentDao, err := DaoBuilder[*Department]{
		DB:          db,
		InsertStmt:  &DaoExecStmt{Query: insertSQL, Cache: false},
		UpdateStmt:  &DaoExecStmt{Query: updateSQL, Cache: false},
		GetByIdStmt: &DaoQueryOneStmt[*Department]{Query: getByIDSQL, Cache: true},
		ListAllStmt: &DaoQueryStmt[*Department]{Query: listAllSQL, Cache: false},
		ListAllPageStmt: &DaoQueryPageStmt[*Department]{
			QueryStmt: &DaoQueryStmt[*Department]{Query: listAllPageSQL, Cache: true},
			CountStmt: &DaoQueryValStmt[int]{Query: countAllSQL, Cache: true},
		},
		DeleteByIdStmt: &DaoExecStmt{Query: deleteByIDSQL, Cache: false},
		NewReceiver:    newReceiver,
		Receive:        receive,
		InsertArgs:     func(d *Department) []any { return []any{d.ID, d.Name, d.Version} },
		UpdateArgs:     func(d *Department) []any { return []any{d.Name, d.Version, d.ID} },
		SaveChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		LoadChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		DeleteChildren: func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
	}.Build(ctx)

	if err != nil {
		t.Fatalf("Failed to create DAO: %v", err)
	}
	return departmentDao
}

func newStudentDao(t *testing.T, db *sql.DB, departmentDao Dao[*Department]) Dao[*Student] {
	// SQL statements for Student operations
	const (
		insertSQL      = `INSERT INTO students (id, name, department_id, version) VALUES (?, ?, ?, ?)`
		updateSQL      = `UPDATE students SET name = ?, department_id = ?, version = ? WHERE id = ?`
		getByIDSQL     = `SELECT id, name, department_id, version FROM students WHERE id = ?`
		listAllSQL     = `SELECT id, name, department_id, version FROM students`
		countAllSQL    = `SELECT COUNT(*) FROM students`
		listAllPageSQL = `SELECT id, name, department_id, version FROM students ORDER BY name LIMIT ? OFFSET ?`
		deleteByIDSQL  = `DELETE FROM students WHERE id = ?`
	)

	// Create DAO instance
	newReceiver := func() *Student { return &Student{Department: &Department{}} }
	receive := func(s *Student) []any {
		return []any{&s.ID, &s.Name, &s.Department.ID, &s.Version}
	}
	studentDao, err := DaoBuilder[*Student]{
		DB:          db,
		InsertStmt:  &DaoExecStmt{Query: insertSQL, Cache: false},
		UpdateStmt:  &DaoExecStmt{Query: updateSQL, Cache: false},
		GetByIdStmt: &DaoQueryOneStmt[*Student]{Query: getByIDSQL, Cache: true},
		ListAllStmt: &DaoQueryStmt[*Student]{Query: listAllSQL, Cache: false},
		ListAllPageStmt: &DaoQueryPageStmt[*Student]{
			QueryStmt: &DaoQueryStmt[*Student]{Query: listAllPageSQL, Cache: true},
			CountStmt: &DaoQueryValStmt[int]{Query: countAllSQL, Cache: true},
		},
		DeleteByIdStmt: &DaoExecStmt{Query: deleteByIDSQL, Cache: false},
		NewReceiver:    newReceiver,
		Receive:        receive,
		InsertArgs:     func(s *Student) []any { return []any{s.ID, s.Name, s.Department.ID, s.Version} },
		UpdateArgs:     func(s *Student) []any { return []any{s.Name, s.Department.ID, s.Version, s.ID} },
		SaveChildren:   func(ctx context.Context, tx *sql.Tx, e *Student) error { return nil },
		LoadChildren: func(ctx context.Context, tx *sql.Tx, s *Student) error {
			if s.Department == nil {
				s.Department = &Department{}
			}
			dept, err := departmentDao.FindById(ctx, s.Department.ID)
			if err != nil {
				return err
			}
			s.Department = dept
			return nil
		},
		DeleteChildren: func(ctx context.Context, tx *sql.Tx, s *Student) error {
			if s.Department != nil {
				return departmentDao.Delete(ctx, s.Department)
			}
			return nil
		},
	}.Build(ctx)

	if err != nil {
		t.Fatalf("Failed to create DAO: %v", err)
	}

	return studentDao
}

func TestDepartmentDao(t *testing.T) {
	// Set up SQLite database
	db := initDB(t)
	defer db.Close()

	departmentDao := newDepartmentDao(t, db)

	// Test Create
	dept := &Department{
		Name: "Computer Science",
	}
	err := departmentDao.Save(ctx, dept)
	if err != nil {
		t.Fatalf("Failed to create department: %v", err)
	}
	if dept.ID == uuid.Nil {
		t.Error("Expected department ID to be set after save")
	}
	if dept.Version == uuid.Nil {
		t.Error("Expected department version to be set after save")
	}

	// Test Read
	fetchedDept, err := departmentDao.FindById(ctx, dept.ID)
	if err != nil {
		t.Fatalf("Failed to fetch department: %v", err)
	}
	if fetchedDept == nil {
		t.Fatal("Expected to find department but got nil")
	}
	if fetchedDept.Name != dept.Name {
		t.Errorf("Expected department name %s, got %s", dept.Name, fetchedDept.Name)
	}

	// Test Update
	originalVersion := dept.Version
	dept.Name = "Data Science"
	err = departmentDao.Save(ctx, dept)
	if err != nil {
		t.Fatalf("Failed to update department: %v", err)
	}
	if dept.Version == originalVersion {
		t.Error("Expected version to change after update")
	}

	// Verify update
	fetchedDept, err = departmentDao.FindById(ctx, dept.ID)
	if err != nil {
		t.Fatalf("Failed to fetch updated department: %v", err)
	}
	if fetchedDept.Name != "Data Science" {
		t.Errorf("Expected updated name 'Data Science', got '%s'", fetchedDept.Name)
	}

	// Test List
	dept2 := &Department{
		Name: "Physics",
	}
	err = departmentDao.Save(ctx, dept2)
	if err != nil {
		t.Fatalf("Failed to create second department: %v", err)
	}

	// Test ListAll
	departments, err := departmentDao.ListAll(ctx)
	if err != nil {
		t.Fatalf("Failed to list departments: %v", err)
	}
	if len(departments) != 2 {
		t.Errorf("Expected 2 departments, got %d", len(departments))
	}

	// Test Pagination
	page, err := departmentDao.ListPage(ctx, Paging{PageNum: 1, PageSize: 1})
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}
	if len(page.Items) != 1 {
		t.Errorf("Expected 1 department per page, got %d", len(page.Items))
	}
	if page.TotalPages != 2 {
		t.Errorf("Expected 2 total pages, got %d", page.TotalPages)
	}

	// Test no update when equal
	originalVersion = dept2.Version
	err = departmentDao.Save(ctx, dept2)
	if err != nil {
		t.Fatalf("Failed to save unchanged department: %v", err)
	}
	if dept2.Version != originalVersion {
		t.Error("Expected version to remain unchanged when department wasn't modified")
	}

	// Test Delete
	err = departmentDao.Delete(ctx, dept)
	if err != nil {
		t.Fatalf("Failed to delete department: %v", err)
	}

	// Verify deletion
	fetchedDept, err = departmentDao.FindById(ctx, dept.ID)
	if err != nil && err != sql.ErrNoRows {
		t.Fatalf("Unexpected error when fetching deleted department: %v", err)
	}
	if fetchedDept != nil {
		t.Error("Expected nil for deleted department")
	}

	// Test version mismatch
	dept2Copy := *dept2
	dept2Copy.SetVersion(uuid.New()) // Change version to force mismatch
	dept2Copy.Name = "Natural Science"
	err = departmentDao.Save(ctx, &dept2Copy)
	if err != ErrVersionMismatch {
		t.Errorf("Expected ErrVersionMismatch, got %v", err)
	}

	// Test bulk operations
	dpt1 := &Department{
		Name: "Dpt1",
	}
	dpt2 := &Department{
		Name: "Dpt2",
	}
	dpt3 := &Department{
		Name: "Dpt3",
	}
	if err := departmentDao.Save(ctx, dpt1, dpt2, dpt3); err != nil {
		t.Fatalf("Failed to save departments: %v", err)
	}

	departments, err = departmentDao.ListAll(ctx)
	if err != nil {
		t.Fatalf("Failed to list departments: %v", err)
	}
	if len(departments) != 4 {
		t.Errorf("Expected 4 departments, got %d", len(departments))
	}

	// Test deleting multiple departments
	err = departmentDao.Delete(ctx, dpt1, dpt2, dpt3)
	if err != nil {
		t.Errorf("Failed to delete departments: %v", err)
	}

	// Verify deletion
	departments, err = departmentDao.ListAll(ctx)
	if err != nil {
		t.Fatalf("Failed to list remaining departments: %v", err)
	}
	if len(departments) != 1 {
		t.Errorf("Expected 0 departments after deletion, got %d", len(departments))
	}

	if err = departmentDao.Close(ctx); err != nil {
		t.Fatalf("Failed to close DAO: %v", err)
	}
}

func TestStudentDao(t *testing.T) {
	// Set up SQLite database
	db := initDB(t)
	defer db.Close()

	departmentDao := newDepartmentDao(t, db)
	studentDao := newStudentDao(t, db, departmentDao)

	// Create a department first
	dept := &Department{
		Name: "Computer Science",
	}
	err := departmentDao.Save(ctx, dept)
	if err != nil {
		t.Fatalf("Failed to create department: %v", err)
	}

	// Test Create
	student := &Student{
		Name:       "John Doe",
		Department: dept,
	}
	err = studentDao.Save(ctx, student)
	if err != nil {
		t.Fatalf("Failed to create student: %v", err)
	}
	if student.ID == uuid.Nil {
		t.Error("Expected student ID to be set after save")
	}
	if student.Version == uuid.Nil {
		t.Error("Expected student version to be set after save")
	}

	// Test Read
	fetchedStudent, err := studentDao.FindById(ctx, student.ID)
	if err != nil {
		t.Fatalf("Failed to fetch student: %v", err)
	}
	if fetchedStudent == nil {
		t.Fatal("Expected to find student but got nil")
	}
	if fetchedStudent.Name != student.Name {
		t.Errorf("Expected student name %s, got %s", student.Name, fetchedStudent.Name)
	}
	if !fetchedStudent.Department.Equals(dept) {
		t.Errorf("Expected department %v, got %v", dept, fetchedStudent.Department)
	}

	// Test Update
	originalVersion := student.Version
	student.Name = "Jane Doe"
	err = studentDao.Save(ctx, student)
	if err != nil {
		t.Fatalf("Failed to update student: %v", err)
	}
	if student.Version == originalVersion {
		t.Error("Expected version to change after update")
	}

	// Verify update
	fetchedStudent, err = studentDao.FindById(ctx, student.ID)
	if err != nil {
		t.Fatalf("Failed to fetch updated student: %v", err)
	}
	if fetchedStudent.Name != "Jane Doe" {
		t.Errorf("Expected updated name 'Jane Doe', got '%s'", fetchedStudent.Name)
	}

	// Create another department
	dept2 := &Department{
		Name: "Physics",
	}
	err = departmentDao.Save(ctx, dept2)
	if err != nil {
		t.Fatalf("Failed to create second department: %v", err)
	}

	// Test department update
	student.Department = dept2
	err = studentDao.Save(ctx, student)
	if err != nil {
		t.Fatalf("Failed to update student department: %v", err)
	}

	// Verify department update
	fetchedStudent, err = studentDao.FindById(ctx, student.ID)
	if err != nil {
		t.Fatalf("Failed to fetch student with updated department: %v", err)
	}
	if !fetchedStudent.Department.Equals(dept2) {
		t.Errorf("Expected department %v, got %v", dept2, fetchedStudent.Department)
	}

	// Test List
	student2 := &Student{
		Name:       "Alice Smith",
		Department: dept,
	}
	err = studentDao.Save(ctx, student2)
	if err != nil {
		t.Fatalf("Failed to create second student: %v", err)
	}

	// Test ListAll
	students, err := studentDao.ListAll(ctx)
	if err != nil {
		t.Fatalf("Failed to list students: %v", err)
	}
	if len(students) != 2 {
		t.Errorf("Expected 2 students, got %d", len(students))
	}

	// Test Pagination
	page, err := studentDao.ListPage(ctx, Paging{PageNum: 1, PageSize: 1})
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}
	if len(page.Items) != 1 {
		t.Errorf("Expected 1 student per page, got %d", len(page.Items))
	}
	if page.TotalPages != 2 {
		t.Errorf("Expected 2 total pages, got %d", page.TotalPages)
	}

	// Test no update when equal
	originalVersion = student2.Version
	err = studentDao.Save(ctx, student2)
	if err != nil {
		t.Fatalf("Failed to save unchanged student: %v", err)
	}
	if student2.Version != originalVersion {
		t.Error("Expected version to remain unchanged when student wasn't modified")
	}

	// Test Delete
	err = studentDao.Delete(ctx, student)
	if err != nil {
		t.Fatalf("Failed to delete student: %v", err)
	}

	// Verify deletion
	fetchedStudent, err = studentDao.FindById(ctx, student.ID)
	if err != nil && err != sql.ErrNoRows {
		t.Fatalf("Unexpected error when fetching deleted student: %v", err)
	}
	if fetchedStudent != nil {
		t.Error("Expected nil for deleted student")
	}

	// Test version mismatch
	student2Copy := *student2
	student2Copy.SetVersion(uuid.New()) // Change version to force mismatch
	student2Copy.Name = "Bob Johnson"
	err = studentDao.Save(ctx, &student2Copy)
	if err != ErrVersionMismatch {
		t.Errorf("Expected ErrVersionMismatch, got %v", err)
	}

	// Test bulk operations
	s1 := &Student{
		Name:       "Student1",
		Department: dept,
	}
	s2 := &Student{
		Name:       "Student2",
		Department: dept,
	}
	s3 := &Student{
		Name:       "Student3",
		Department: dept2,
	}
	if err := studentDao.Save(ctx, s1, s2, s3); err != nil {
		t.Fatalf("Failed to save students: %v", err)
	}

	students, err = studentDao.ListAll(ctx)
	if err != nil {
		t.Fatalf("Failed to list students: %v", err)
	}
	if len(students) != 4 {
		t.Errorf("Expected 4 students, got %d", len(students))
	}

	// Test deleting multiple students
	err = studentDao.Delete(ctx, s1, s2, s3)
	if err != nil {
		t.Errorf("Failed to delete students: %v", err)
	}

	// Verify deletion
	students, err = studentDao.ListAll(ctx)
	if err != nil {
		t.Fatalf("Failed to list remaining students: %v", err)
	}
	if len(students) != 1 {
		t.Errorf("Expected 1 student after deletion, got %d", len(students))
	}

	// Test cascade deletion
	cascadeDept := &Department{
		Name: "Mathematics",
	}
	err = departmentDao.Save(ctx, cascadeDept)
	if err != nil {
		t.Fatalf("Failed to create cascade test department: %v", err)
	}

	cascadeStudent := &Student{
		Name:       "Cascade Test Student",
		Department: cascadeDept,
	}
	err = studentDao.Save(ctx, cascadeStudent)
	if err != nil {
		t.Fatalf("Failed to create cascade test student: %v", err)
	}

	// Verify department exists
	_, err = departmentDao.FindById(ctx, cascadeDept.ID)
	if err != nil {
		t.Fatalf("Failed to fetch cascade test department: %v", err)
	}

	// Delete student which should cascade delete the department
	err = studentDao.DeleteCascade(ctx, cascadeStudent)
	if err != nil {
		t.Fatalf("Failed to delete cascade test student: %v", err)
	}

	// Verify department was also deleted
	deletedDept, err := departmentDao.FindById(ctx, cascadeDept.ID)
	if err != nil && err != sql.ErrNoRows {
		t.Fatalf("Unexpected error when fetching deleted department: %v", err)
	}
	if deletedDept != nil {
		t.Error("Expected nil for cascade deleted department")
	}

	if err = studentDao.Close(ctx); err != nil {
		t.Fatalf("Failed to close DAO: %v", err)
	}
}

func TestDaoBuilderValidate(t *testing.T) {
	// Set up SQLite database
	db := initDB(t)
	defer db.Close()

	// Test missing DB
	_, err := DaoBuilder[*Department]{
		InsertStmt:     &DaoExecStmt{Query: "INSERT INTO departments VALUES (?, ?, ?)", Cache: false},
		UpdateStmt:     &DaoExecStmt{Query: "UPDATE departments SET name = ? WHERE id = ?", Cache: false},
		GetByIdStmt:    &DaoQueryOneStmt[*Department]{Query: "SELECT * FROM departments WHERE id = ?", Cache: true},
		ListAllStmt:    &DaoQueryStmt[*Department]{Query: "SELECT * FROM departments", Cache: false},
		DeleteByIdStmt: &DaoExecStmt{Query: "DELETE FROM departments WHERE id = ?", Cache: false},
		InsertArgs:     func(d *Department) []any { return []any{d.ID, d.Name, d.Version} },
		UpdateArgs:     func(d *Department) []any { return []any{d.Name, d.ID} },
		SaveChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		LoadChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		DeleteChildren: func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
	}.Build(ctx)

	if err == nil {
		t.Error("Expected error for missing DB, got nil")
	}

	// Test missing InsertStmt
	_, err = DaoBuilder[*Department]{
		DB:             db,
		UpdateStmt:     &DaoExecStmt{Query: "UPDATE departments SET name = ? WHERE id = ?", Cache: false},
		GetByIdStmt:    &DaoQueryOneStmt[*Department]{Query: "SELECT * FROM departments WHERE id = ?", Cache: true},
		ListAllStmt:    &DaoQueryStmt[*Department]{Query: "SELECT * FROM departments", Cache: false},
		DeleteByIdStmt: &DaoExecStmt{Query: "DELETE FROM departments WHERE id = ?", Cache: false},
		InsertArgs:     func(d *Department) []any { return []any{d.ID, d.Name, d.Version} },
		UpdateArgs:     func(d *Department) []any { return []any{d.Name, d.ID} },
		SaveChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		LoadChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		DeleteChildren: func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
	}.Build(ctx)

	if err == nil {
		t.Error("Expected error for missing InsertStmt, got nil")
	}

	// Test missing UpdateStmt
	_, err = DaoBuilder[*Department]{
		DB:             db,
		InsertStmt:     &DaoExecStmt{Query: "INSERT INTO departments VALUES (?, ?, ?)", Cache: false},
		GetByIdStmt:    &DaoQueryOneStmt[*Department]{Query: "SELECT * FROM departments WHERE id = ?", Cache: true},
		ListAllStmt:    &DaoQueryStmt[*Department]{Query: "SELECT * FROM departments", Cache: false},
		DeleteByIdStmt: &DaoExecStmt{Query: "DELETE FROM departments WHERE id = ?", Cache: false},
		InsertArgs:     func(d *Department) []any { return []any{d.ID, d.Name, d.Version} },
		UpdateArgs:     func(d *Department) []any { return []any{d.Name, d.ID} },
		SaveChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		LoadChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		DeleteChildren: func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
	}.Build(ctx)

	if err == nil {
		t.Error("Expected error for missing UpdateStmt, got nil")
	}

	// Test missing GetByIdStmt
	_, err = DaoBuilder[*Department]{
		DB:             db,
		InsertStmt:     &DaoExecStmt{Query: "INSERT INTO departments VALUES (?, ?, ?)", Cache: false},
		UpdateStmt:     &DaoExecStmt{Query: "UPDATE departments SET name = ? WHERE id = ?", Cache: false},
		ListAllStmt:    &DaoQueryStmt[*Department]{Query: "SELECT * FROM departments", Cache: false},
		DeleteByIdStmt: &DaoExecStmt{Query: "DELETE FROM departments WHERE id = ?", Cache: false},
		InsertArgs:     func(d *Department) []any { return []any{d.ID, d.Name, d.Version} },
		UpdateArgs:     func(d *Department) []any { return []any{d.Name, d.ID} },
		SaveChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		LoadChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		DeleteChildren: func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
	}.Build(ctx)

	if err == nil {
		t.Error("Expected error for missing GetByIdStmt, got nil")
	}

	// Test missing ListAllStmt
	_, err = DaoBuilder[*Department]{
		DB:             db,
		InsertStmt:     &DaoExecStmt{Query: "INSERT INTO departments VALUES (?, ?, ?)", Cache: false},
		UpdateStmt:     &DaoExecStmt{Query: "UPDATE departments SET name = ? WHERE id = ?", Cache: false},
		GetByIdStmt:    &DaoQueryOneStmt[*Department]{Query: "SELECT * FROM departments WHERE id = ?", Cache: true},
		DeleteByIdStmt: &DaoExecStmt{Query: "DELETE FROM departments WHERE id = ?", Cache: false},
		InsertArgs:     func(d *Department) []any { return []any{d.ID, d.Name, d.Version} },
		UpdateArgs:     func(d *Department) []any { return []any{d.Name, d.ID} },
		SaveChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		LoadChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		DeleteChildren: func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
	}.Build(ctx)

	if err == nil {
		t.Error("Expected error for missing ListAllStmt, got nil")
	}

	// Test missing DeleteByIdStmt
	_, err = DaoBuilder[*Department]{
		DB:             db,
		InsertStmt:     &DaoExecStmt{Query: "INSERT INTO departments VALUES (?, ?, ?)", Cache: false},
		UpdateStmt:     &DaoExecStmt{Query: "UPDATE departments SET name = ? WHERE id = ?", Cache: false},
		GetByIdStmt:    &DaoQueryOneStmt[*Department]{Query: "SELECT * FROM departments WHERE id = ?", Cache: true},
		ListAllStmt:    &DaoQueryStmt[*Department]{Query: "SELECT * FROM departments", Cache: false},
		InsertArgs:     func(d *Department) []any { return []any{d.ID, d.Name, d.Version} },
		UpdateArgs:     func(d *Department) []any { return []any{d.Name, d.ID} },
		SaveChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		LoadChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		DeleteChildren: func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
	}.Build(ctx)

	if err == nil {
		t.Error("Expected error for missing DeleteByIdStmt, got nil")
	}

	// Test missing InsertArgs
	_, err = DaoBuilder[*Department]{
		DB:             db,
		InsertStmt:     &DaoExecStmt{Query: "INSERT INTO departments VALUES (?, ?, ?)", Cache: false},
		UpdateStmt:     &DaoExecStmt{Query: "UPDATE departments SET name = ? WHERE id = ?", Cache: false},
		GetByIdStmt:    &DaoQueryOneStmt[*Department]{Query: "SELECT * FROM departments WHERE id = ?", Cache: true},
		ListAllStmt:    &DaoQueryStmt[*Department]{Query: "SELECT * FROM departments", Cache: false},
		DeleteByIdStmt: &DaoExecStmt{Query: "DELETE FROM departments WHERE id = ?", Cache: false},
		UpdateArgs:     func(d *Department) []any { return []any{d.Name, d.ID} },
		SaveChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		LoadChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		DeleteChildren: func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
	}.Build(ctx)

	if err == nil {
		t.Error("Expected error for missing InsertArgs, got nil")
	}

	// Test missing UpdateArgs
	_, err = DaoBuilder[*Department]{
		DB:             db,
		InsertStmt:     &DaoExecStmt{Query: "INSERT INTO departments VALUES (?, ?, ?)", Cache: false},
		UpdateStmt:     &DaoExecStmt{Query: "UPDATE departments SET name = ? WHERE id = ?", Cache: false},
		GetByIdStmt:    &DaoQueryOneStmt[*Department]{Query: "SELECT * FROM departments WHERE id = ?", Cache: true},
		ListAllStmt:    &DaoQueryStmt[*Department]{Query: "SELECT * FROM departments", Cache: false},
		DeleteByIdStmt: &DaoExecStmt{Query: "DELETE FROM departments WHERE id = ?", Cache: false},
		InsertArgs:     func(d *Department) []any { return []any{d.ID, d.Name, d.Version} },
		SaveChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		LoadChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		DeleteChildren: func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
	}.Build(ctx)

	if err == nil {
		t.Error("Expected error for missing UpdateArgs, got nil")
	}

	// Test missing SaveChildren
	_, err = DaoBuilder[*Department]{
		DB:             db,
		InsertStmt:     &DaoExecStmt{Query: "INSERT INTO departments VALUES (?, ?, ?)", Cache: false},
		UpdateStmt:     &DaoExecStmt{Query: "UPDATE departments SET name = ? WHERE id = ?", Cache: false},
		GetByIdStmt:    &DaoQueryOneStmt[*Department]{Query: "SELECT * FROM departments WHERE id = ?", Cache: true},
		ListAllStmt:    &DaoQueryStmt[*Department]{Query: "SELECT * FROM departments", Cache: false},
		DeleteByIdStmt: &DaoExecStmt{Query: "DELETE FROM departments WHERE id = ?", Cache: false},
		InsertArgs:     func(d *Department) []any { return []any{d.ID, d.Name, d.Version} },
		UpdateArgs:     func(d *Department) []any { return []any{d.Name, d.ID} },
		LoadChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		DeleteChildren: func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
	}.Build(ctx)

	if err == nil {
		t.Error("Expected error for missing SaveChildren, got nil")
	}

	// Test missing LoadChildren
	_, err = DaoBuilder[*Department]{
		DB:             db,
		InsertStmt:     &DaoExecStmt{Query: "INSERT INTO departments VALUES (?, ?, ?)", Cache: false},
		UpdateStmt:     &DaoExecStmt{Query: "UPDATE departments SET name = ? WHERE id = ?", Cache: false},
		GetByIdStmt:    &DaoQueryOneStmt[*Department]{Query: "SELECT * FROM departments WHERE id = ?", Cache: true},
		ListAllStmt:    &DaoQueryStmt[*Department]{Query: "SELECT * FROM departments", Cache: false},
		DeleteByIdStmt: &DaoExecStmt{Query: "DELETE FROM departments WHERE id = ?", Cache: false},
		InsertArgs:     func(d *Department) []any { return []any{d.ID, d.Name, d.Version} },
		UpdateArgs:     func(d *Department) []any { return []any{d.Name, d.ID} },
		SaveChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		DeleteChildren: func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
	}.Build(ctx)

	if err == nil {
		t.Error("Expected error for missing LoadChildren, got nil")
	}

	// Test missing DeleteChildren
	_, err = DaoBuilder[*Department]{
		DB:             db,
		InsertStmt:     &DaoExecStmt{Query: "INSERT INTO departments VALUES (?, ?, ?)", Cache: false},
		UpdateStmt:     &DaoExecStmt{Query: "UPDATE departments SET name = ? WHERE id = ?", Cache: false},
		GetByIdStmt:    &DaoQueryOneStmt[*Department]{Query: "SELECT * FROM departments WHERE id = ?", Cache: true},
		ListAllStmt:    &DaoQueryStmt[*Department]{Query: "SELECT * FROM departments", Cache: false},
		DeleteByIdStmt: &DaoExecStmt{Query: "DELETE FROM departments WHERE id = ?", Cache: false},
		InsertArgs:     func(d *Department) []any { return []any{d.ID, d.Name, d.Version} },
		UpdateArgs:     func(d *Department) []any { return []any{d.Name, d.ID} },
		SaveChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		LoadChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
	}.Build(ctx)

	if err == nil {
		t.Error("Expected error for missing DeleteChildren, got nil")
	}

	// Test valid builder
	newReceiver := func() *Department { return &Department{} }
	receive := func(d *Department) []any { return []any{&d.ID, &d.Name, &d.Version} }

	_, err = DaoBuilder[*Department]{
		DB:             db,
		InsertStmt:     &DaoExecStmt{Query: "INSERT INTO departments VALUES (?, ?, ?)", Cache: false},
		UpdateStmt:     &DaoExecStmt{Query: "UPDATE departments SET name = ? WHERE id = ?", Cache: false},
		GetByIdStmt:    &DaoQueryOneStmt[*Department]{Query: "SELECT * FROM departments WHERE id = ?", Cache: true},
		ListAllStmt:    &DaoQueryStmt[*Department]{Query: "SELECT * FROM departments", Cache: false},
		DeleteByIdStmt: &DaoExecStmt{Query: "DELETE FROM departments WHERE id = ?", Cache: false},
		InsertArgs:     func(d *Department) []any { return []any{d.ID, d.Name, d.Version} },
		UpdateArgs:     func(d *Department) []any { return []any{d.Name, d.ID} },
		SaveChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		LoadChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		DeleteChildren: func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
	}.Build(ctx)

	if err == nil {
		t.Error("Expected error for missing ListAllPageStmt, got nil")
	}

	_, err = DaoBuilder[*Department]{
		DB: db,
		ListAllPageStmt: &DaoQueryPageStmt[*Department]{
			QueryStmt: &DaoQueryStmt[*Department]{Query: "SELECT * FROM departments", Cache: false},
			CountStmt: &DaoQueryValStmt[int]{Query: "SELECT COUNT(*) FROM departments", Cache: true},
		},
		InsertStmt:     &DaoExecStmt{Query: "INSERT INTO departments VALUES (?, ?, ?)", Cache: false},
		UpdateStmt:     &DaoExecStmt{Query: "UPDATE departments SET name = ? WHERE id = ?", Cache: false},
		GetByIdStmt:    &DaoQueryOneStmt[*Department]{Query: "SELECT * FROM departments WHERE id = ?", Cache: true},
		ListAllStmt:    &DaoQueryStmt[*Department]{Query: "SELECT * FROM departments", Cache: false},
		DeleteByIdStmt: &DaoExecStmt{Query: "DELETE FROM departments WHERE id = ?", Cache: false},
		InsertArgs:     func(d *Department) []any { return []any{d.ID, d.Name, d.Version} },
		Receive:        receive,
		UpdateArgs:     func(d *Department) []any { return []any{d.Name, d.ID} },
		SaveChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		LoadChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		DeleteChildren: func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
	}.Build(ctx)

	if err == nil {
		t.Errorf("Expected builder error on missing newReceiver, got nil")
	}

	_, err = DaoBuilder[*Department]{
		DB: db,
		ListAllPageStmt: &DaoQueryPageStmt[*Department]{
			QueryStmt: &DaoQueryStmt[*Department]{Query: "SELECT * FROM departments", Cache: false},
			CountStmt: &DaoQueryValStmt[int]{Query: "SELECT COUNT(*) FROM departments", Cache: true},
		},
		InsertStmt:     &DaoExecStmt{Query: "INSERT INTO departments VALUES (?, ?, ?)", Cache: false},
		UpdateStmt:     &DaoExecStmt{Query: "UPDATE departments SET name = ? WHERE id = ?", Cache: false},
		GetByIdStmt:    &DaoQueryOneStmt[*Department]{Query: "SELECT * FROM departments WHERE id = ?", Cache: true},
		ListAllStmt:    &DaoQueryStmt[*Department]{Query: "SELECT * FROM departments", Cache: false},
		DeleteByIdStmt: &DaoExecStmt{Query: "DELETE FROM departments WHERE id = ?", Cache: false},
		InsertArgs:     func(d *Department) []any { return []any{d.ID, d.Name, d.Version} },
		NewReceiver:    newReceiver,
		UpdateArgs:     func(d *Department) []any { return []any{d.Name, d.ID} },
		SaveChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		LoadChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		DeleteChildren: func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
	}.Build(ctx)

	if err == nil {
		t.Errorf("Expected builder error on missing recive, got nil")
	}

	_, err = DaoBuilder[*Department]{
		DB: db,
		ListAllPageStmt: &DaoQueryPageStmt[*Department]{
			QueryStmt: &DaoQueryStmt[*Department]{Query: "SELECT * FROM departments", Cache: false},
			CountStmt: &DaoQueryValStmt[int]{Query: "SELECT COUNT(*) FROM departments", Cache: true},
		},
		InsertStmt:     &DaoExecStmt{Query: "INSERT INTO departments VALUES (?, ?, ?)", Cache: false},
		UpdateStmt:     &DaoExecStmt{Query: "UPDATE departments SET name = ? WHERE id = ?", Cache: false},
		GetByIdStmt:    &DaoQueryOneStmt[*Department]{Query: "SELECT * FROM departments WHERE id = ?", Cache: true},
		ListAllStmt:    &DaoQueryStmt[*Department]{Query: "SELECT * FROM departments", Cache: false},
		DeleteByIdStmt: &DaoExecStmt{Query: "DELETE FROM departments WHERE id = ?", Cache: false},
		InsertArgs:     func(d *Department) []any { return []any{d.ID, d.Name, d.Version} },
		NewReceiver:    newReceiver,
		Receive:        receive,
		UpdateArgs:     func(d *Department) []any { return []any{d.Name, d.ID} },
		SaveChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		LoadChildren:   func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
		DeleteChildren: func(ctx context.Context, tx *sql.Tx, e *Department) error { return nil },
	}.Build(ctx)

	if err != nil {
		t.Errorf("Expected no error for valid builder, got: %v", err)
	}
}
