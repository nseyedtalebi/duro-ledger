// Package pgtest hands Duro's integration tests disposable PostgreSQL
// databases and login roles. Every test gets its own database and its own
// role names, so packages can run in parallel without colliding, and both
// are dropped when the test finishes.
package pgtest

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// DSNEnv names the environment variable holding an administrator DSN for a
// disposable PostgreSQL 18+ cluster.
const DSNEnv = "DURO_POSTGRES_TEST_DSN"

// RolePassword is the password given to every test login role. These roles
// exist only inside a disposable test cluster.
const RolePassword = "duro-test-password"

var seq atomic.Int64

// AdminDSN returns the administrator DSN, failing the test if it is unset:
// Duro's acceptance gate does not count skipped integration tests as
// passes, so an absent database is a failure, not a skip.
func AdminDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv(DSNEnv)
	if dsn == "" {
		t.Fatalf("%s is not set: these tests require a live disposable PostgreSQL 18+ cluster, and a skipped integration test is not a pass", DSNEnv)
	}
	return dsn
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, os.Getpid(), seq.Add(1))
}

// withDatabase returns dsn pointed at a different database name.
func withDatabase(t *testing.T, dsn, name string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parsing %s: %v", DSNEnv, err)
	}
	u.Path = "/" + name
	return u.String()
}

func exec(t *testing.T, dsn, stmt string) error {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(stmt)
	return err
}

// NewDatabase creates a fresh, uniquely named database and returns an
// administrator DSN for it. The database is force-dropped when the test
// ends, so tests never share (or destroy) each other's ledger.
func NewDatabase(t *testing.T) string {
	t.Helper()
	admin := AdminDSN(t)
	name := uniqueName("duro_test")
	if err := exec(t, admin, `CREATE DATABASE `+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("creating test database: %v", err)
	}
	dsn := withDatabase(t, admin, name)
	t.Cleanup(func() {
		if err := exec(t, admin, fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`, pgx.Identifier{name}.Sanitize())); err != nil {
			t.Errorf("dropping test database %s: %v", name, err)
		}
	})
	return dsn
}

// NewRole creates a uniquely named login role with no privileges and
// returns its name and a DSN connecting as it to the same database as
// adminDSN. Cleanup drops everything the role owns in that database and
// then the role itself.
func NewRole(t *testing.T, adminDSN string) (role, dsn string) {
	t.Helper()
	role = uniqueName("duro_role")
	ident := pgx.Identifier{role}.Sanitize()
	if err := exec(t, adminDSN, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s'`, ident, RolePassword)); err != nil {
		t.Fatalf("creating test role: %v", err)
	}
	t.Cleanup(func() {
		// Privileges granted inside this database are dependencies of the
		// role; drop them before the role. The database may already be
		// gone, in which case so are the grants.
		_ = exec(t, adminDSN, fmt.Sprintf(`DROP OWNED BY %s CASCADE`, ident))
		if err := exec(t, AdminDSN(t), fmt.Sprintf(`DROP ROLE IF EXISTS %s`, ident)); err != nil {
			t.Errorf("dropping test role %s: %v", role, err)
		}
	})

	u, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("parsing DSN: %v", err)
	}
	u.User = url.UserPassword(role, RolePassword)
	return role, u.String()
}
