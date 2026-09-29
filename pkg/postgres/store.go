// Package postgres is Duro's canonical event store. PostgreSQL is the sole
// authority for accepted events: it generates each event's id (UUIDv7) and
// received_at, and binds actor to the authenticated login role. This
// package never overwrites or deletes a canonical row.
package postgres

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "embed"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/nseyedtalebi/duro-ledger/pkg/event"
)

//go:embed schema.sql
var schema string

// StoredEvent is a canonical event exactly as PostgreSQL committed it.
type StoredEvent struct {
	ID         string
	ReceivedAt time.Time
	EventType  string
	Actor      string
	Content    json.RawMessage
	Refs       json.RawMessage
}

// CommitOutcome classifies why Append failed to return a StoredEvent.
type CommitOutcome int

const (
	// OutcomeNotCommitted means PostgreSQL is known not to have committed
	// the event: validation failed before any database access, or the
	// database itself returned a definite error (constraint violation,
	// rejected statement, commit that resulted in rollback) in response to
	// a request Duro sent and got an answer for.
	OutcomeNotCommitted CommitOutcome = iota
	// OutcomeUnknown means the connection was lost after the COMMIT was
	// sent but before Duro received a response: PostgreSQL may have
	// committed before the acknowledgement was lost. Duro never
	// automatically resubmits; retrying is an explicit caller action.
	OutcomeUnknown
)

func (o CommitOutcome) String() string {
	if o == OutcomeUnknown {
		return "unknown"
	}
	return "not_committed"
}

// AppendError reports that Append did not return a committed event, along
// with which of the two boundary outcomes applies.
type AppendError struct {
	Outcome CommitOutcome
	Err     error
}

func (e *AppendError) Error() string { return fmt.Sprintf("postgres: append %s: %v", e.Outcome, e.Err) }
func (e *AppendError) Unwrap() error { return e.Err }

// Store is the PostgreSQL-backed canonical event store.
type Store struct {
	db *sql.DB
}

// Open connects with an ordinary (non-administrator) DSN. It does not run
// DDL or grant privileges: those require an administrator/provisioning
// connection via Initialize, ProvisionWriter, and ProvisionReader.
func Open(dsn string) (*Store, error) {
	db, err := openDB(dsn)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error { return s.db.Close() }

func openDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// initializeLockKey serializes concurrent Initialize calls against each
// other for the whole transaction, so two racing initializers cannot both
// run the CREATE TABLE (which, under concurrency, fails with a catalog
// unique-violation rather than honoring IF NOT EXISTS).
const initializeLockKey = 5138024775510001

// referenceTable is where Initialize builds a throwaway copy of the current
// contract schema to compare an existing public.events against. It lives in
// pg_temp and inside Initialize's transaction, so it is never visible to
// anyone else and never outlives the check.
const referenceTable = "pg_temp.duro_events_reference"

// Initialize applies Duro's schema using an administrator/provisioning
// connection, in one transaction. It is idempotent: run against an
// already-initialized, contract-compliant database it does nothing and
// succeeds. Run against a database whose public.events differs from the
// current contract in any way Initialize can see -- columns, types,
// nullability, defaults, primary key, CHECK constraints, relation kind,
// persistence, triggers, rules, or row-level security -- it returns an error
// and changes nothing. It never drops, alters, or converts an existing table
// or its data.
func Initialize(dsn string) error {
	db, err := openDB(dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	var encoding string
	if err := db.QueryRow(`SHOW server_encoding`).Scan(&encoding); err != nil {
		return err
	}
	if encoding != "UTF8" {
		return fmt.Errorf("postgres: database server_encoding is %s; Duro requires UTF8", encoding)
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`SELECT pg_catalog.pg_advisory_xact_lock($1)`, initializeLockKey); err != nil {
		return err
	}

	var exists bool
	if err := tx.QueryRow(`SELECT pg_catalog.to_regclass('public.events') IS NOT NULL`).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		if _, err := tx.Exec(schema); err != nil {
			return err
		}
		return tx.Commit()
	}

	// Column descriptors cannot express these: an unlogged table loses every
	// event on crash recovery, a view is not the canonical store, and a user
	// trigger or rule can rewrite or silently drop an append. Check them
	// explicitly (the temporary reference table below is itself temporary,
	// so its persistence cannot be compared directly).
	var relkind, relpersistence string
	var rowSecurity, forceRowSecurity bool
	var policies int
	if err := tx.QueryRow(
		`SELECT c.relkind::text, c.relpersistence::text, c.relrowsecurity, c.relforcerowsecurity,
		        (SELECT count(*) FROM pg_catalog.pg_policy p WHERE p.polrelid = c.oid)
		   FROM pg_catalog.pg_class c
		  WHERE c.oid = 'public.events'::pg_catalog.regclass`,
	).Scan(&relkind, &relpersistence, &rowSecurity, &forceRowSecurity, &policies); err != nil {
		return err
	}
	if relkind != "r" {
		return fmt.Errorf("postgres: public.events is not an ordinary table (pg_class.relkind = %q); refusing to alter or drop it", relkind)
	}
	if relpersistence != "p" {
		return fmt.Errorf("postgres: public.events is not a permanent table (pg_class.relpersistence = %q; unlogged or temporary tables do not survive a crash); refusing to alter or drop it", relpersistence)
	}
	// Row-level security turns the canonical table into a per-role filtered
	// projection: a reader could be shown a subset of the ledger as if it were
	// the whole thing, and a policy's USING/WITH CHECK can reject an append
	// the schema itself accepts. Neither is the standalone contract, so refuse
	// rather than quietly inherit whatever filtering is configured. Enabled
	// RLS with no policy is refused too: it denies every non-owner row.
	if rowSecurity || forceRowSecurity || policies > 0 {
		return fmt.Errorf("postgres: public.events has row-level security (relrowsecurity=%v, relforcerowsecurity=%v, %d policies), which can filter reads and reject appends per role; refusing to alter or drop it", rowSecurity, forceRowSecurity, policies)
	}
	var extras string
	if err := tx.QueryRow(`
		SELECT COALESCE(pg_catalog.string_agg(name, ', '), '') FROM (
		    SELECT 'trigger ' || tgname AS name FROM pg_catalog.pg_trigger
		     WHERE tgrelid = 'public.events'::pg_catalog.regclass AND NOT tgisinternal
		    UNION ALL
		    SELECT 'rule ' || rulename FROM pg_catalog.pg_rewrite
		     WHERE ev_class = 'public.events'::pg_catalog.regclass AND rulename <> '_RETURN'
		) x`).Scan(&extras); err != nil {
		return err
	}
	if extras != "" {
		return fmt.Errorf("postgres: public.events carries %s, which can rewrite or suppress appends; refusing to alter or drop it", extras)
	}

	// Build the contract schema as a temporary table from the same DDL and
	// compare PostgreSQL's own descriptors of the two tables. Comparing
	// catalog descriptors rather than a hand-maintained column list means
	// the check covers everything the DDL says -- defaults, NOT NULL, the
	// primary key, every CHECK -- and cannot drift from schema.sql.
	if _, err := tx.Exec(strings.ReplaceAll(schema, "public.events", referenceTable)); err != nil {
		return err
	}
	want, err := describeTable(tx, referenceTable)
	if err != nil {
		return err
	}
	got, err := describeTable(tx, "public.events")
	if err != nil {
		return err
	}
	if diff := descriptorDiff(want, got); diff != "" {
		// Rollback (deferred) discards the reference table.
		return fmt.Errorf("postgres: public.events already exists and does not match the current contract schema (%s); refusing to alter or drop it (see README for the new-deployment-only upgrade path)", diff)
	}
	return nil // already initialized and compatible: no-op, nothing committed
}

// describeTable returns PostgreSQL's own description of a table as sorted
// text: one line per column (name, type, nullability, default expression)
// and one per constraint definition, which includes the primary key and
// every CHECK.
func describeTable(tx *sql.Tx, qualified string) ([]string, error) {
	var out []string
	for _, q := range []string{
		`SELECT a.attnum || ' ' || a.attname || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod) ||
		        ' notnull=' || a.attnotnull ||
		        ' default=' || COALESCE(pg_catalog.pg_get_expr(d.adbin, d.adrelid), '')
		   FROM pg_catalog.pg_attribute a
		   LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		  WHERE a.attrelid = $1::pg_catalog.regclass AND a.attnum > 0 AND NOT a.attisdropped
		  ORDER BY a.attnum`,
		`SELECT 'constraint ' || pg_catalog.pg_get_constraintdef(c.oid)
		   FROM pg_catalog.pg_constraint c
		  WHERE c.conrelid = $1::pg_catalog.regclass
		  ORDER BY 1`,
	} {
		rows, err := tx.Query(q, qualified)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				rows.Close()
				return nil, err
			}
			out = append(out, line)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return out, nil
}

// descriptorDiff returns "" if the two descriptions are identical, or a
// short human-readable summary of the first disagreement.
func descriptorDiff(want, got []string) string {
	for i, w := range want {
		if i >= len(got) {
			return fmt.Sprintf("missing %q", w)
		}
		if got[i] != w {
			return fmt.Sprintf("expected %q, found %q", w, got[i])
		}
	}
	if len(got) > len(want) {
		return fmt.Sprintf("unexpected %q", got[len(want)])
	}
	return ""
}

// protectedColumns are the server-owned columns a writer must never be able
// to supply. INSERT privilege on any of them (directly, by column grant, by
// a table-wide grant, by inheritance, or by SET ROLE) defeats the whole
// writer boundary, because the value would come from the client instead of
// the column default.
var protectedColumns = []string{"id", "received_at", "actor"}

// eligibilityQuery lists every reason role must not be given a restricted
// grant. It walks the roles reachable from role -- itself, roles it inherits
// from, and roles it can SET ROLE to, which NOINHERIT deliberately hides
// from inherited-privilege checks -- and reports privileged attributes,
// ownership, forbidden table privileges, and INSERT on any server-owned
// column. One bounded query, no role management.
const eligibilityQuery = `
WITH reachable AS (
    SELECT r.rolname, r.rolsuper, r.rolcreaterole, r.rolcreatedb, r.rolbypassrls, r.rolreplication
      FROM pg_catalog.pg_roles r
     WHERE r.rolname = $1
        OR pg_catalog.pg_has_role($1, r.oid, 'USAGE')
        OR pg_catalog.pg_has_role($1, r.oid, 'SET')
)
SELECT reason FROM (
    SELECT rolname || ' is a superuser' AS reason FROM reachable WHERE rolsuper
    UNION ALL SELECT rolname || ' has CREATEROLE' FROM reachable WHERE rolcreaterole
    UNION ALL SELECT rolname || ' has CREATEDB' FROM reachable WHERE rolcreatedb
    UNION ALL SELECT rolname || ' has BYPASSRLS' FROM reachable WHERE rolbypassrls
    UNION ALL SELECT rolname || ' has REPLICATION' FROM reachable WHERE rolreplication
    UNION ALL SELECT rolname || ' owns public.events' FROM reachable
       WHERE rolname = pg_catalog.pg_get_userbyid(
                 (SELECT relowner FROM pg_catalog.pg_class WHERE oid = 'public.events'::pg_catalog.regclass))
    UNION ALL SELECT rolname || ' owns schema public' FROM reachable
       WHERE rolname = pg_catalog.pg_get_userbyid(
                 (SELECT nspowner FROM pg_catalog.pg_namespace WHERE nspname = 'public'))
    UNION ALL SELECT rolname || ' owns this database' FROM reachable
       WHERE rolname = pg_catalog.pg_get_userbyid(
                 (SELECT datdba FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()))
    UNION ALL SELECT rolname || ' holds ' || p FROM reachable, pg_catalog.unnest(ARRAY['UPDATE','DELETE','TRUNCATE','TRIGGER']) AS p
       WHERE pg_catalog.has_table_privilege(rolname, 'public.events', p)
          OR (p = 'UPDATE' AND pg_catalog.has_any_column_privilege(rolname, 'public.events', p))
    UNION ALL SELECT rolname || ' holds INSERT on server-owned column ' || c FROM reachable, pg_catalog.unnest($2::text[]) AS c
       WHERE pg_catalog.has_column_privilege(rolname, 'public.events', c, 'INSERT')
    UNION ALL SELECT rolname || ' holds ' || p FROM reachable, pg_catalog.unnest($3::text[]) AS p
       WHERE pg_catalog.has_any_column_privilege(rolname, 'public.events', p)
) reasons
ORDER BY reason`

// assertProvisionable refuses to hand a restricted grant to a role that
// restricting cannot actually restrict. Granting append privilege to the
// table owner, a superuser, a member of a superuser role, or a role that
// already holds mutation privilege (directly or by inheritance) would
// report a secure writer while leaving the contract's writer boundary
// unenforced, so Provision* fails instead of adding grants. extraPrivileges
// are checked in addition to the always-forbidden ones.
func assertProvisionable(tx *sql.Tx, role string, extraPrivileges ...string) error {
	var canLogin bool
	err := tx.QueryRow(`SELECT rolcanlogin FROM pg_catalog.pg_roles WHERE rolname = $1`, role).Scan(&canLogin)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("postgres: role %q does not exist; create the login role first", role)
	}
	if err != nil {
		return err
	}
	if !canLogin {
		return fmt.Errorf("postgres: role %q cannot log in; actor is the authenticated login identity", role)
	}

	rows, err := tx.Query(eligibilityQuery, role, protectedColumns, extraPrivileges)
	if err != nil {
		return err
	}
	defer rows.Close()
	var reasons []string
	for rows.Next() {
		var reason string
		if err := rows.Scan(&reason); err != nil {
			return err
		}
		reasons = append(reasons, reason)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(reasons) > 0 {
		return fmt.Errorf("postgres: refusing to provision %q: restricted grants cannot restrain authority it already reaches (directly, by inheritance, or by SET ROLE): %s; revoke it or use a separate login role",
			role, strings.Join(reasons, "; "))
	}
	return nil
}

// provision checks eligibility and applies grants in one transaction, so a
// failure part-way through leaves no grants behind. grants are format
// strings taking the quoted role identifier.
func provision(dsn, role string, extraPrivileges []string, grants ...string) error {
	ident, err := quoteIdentifier(role)
	if err != nil {
		return err
	}
	db, err := openDB(dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Checked inside the granting transaction so the check and the grants are
	// one atomic unit: no partial grants, and no grants applied over a failed
	// check. This does not lock privileges against an administrator -- a
	// superuser or CREATEROLE role can grant this role new authority
	// concurrently or immediately afterwards, and the administrator is
	// outside the writer boundary by design.
	if err := assertProvisionable(tx, role, extraPrivileges...); err != nil {
		return err
	}
	for _, stmt := range grants {
		if _, err := tx.Exec(fmt.Sprintf(stmt, ident)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ProvisionWriter grants an existing login role the minimum privilege
// needed to append events: column-level INSERT on event_type/content/refs
// only, plus table-level SELECT. Because id, received_at, and actor have no
// INSERT grant, this role physically cannot forge them; PostgreSQL supplies
// its defaults (pg_catalog.uuidv7(), clock_timestamp(), session_user)
// instead. SELECT is required too: INSERT ... RETURNING needs read
// privilege on the columns it returns, and Append relies on RETURNING to
// hand back the complete stored event after commit. SELECT grants no
// INSERT/UPDATE/DELETE, so the role still cannot modify or remove existing
// rows. Run with an administrator/provisioning DSN.
func ProvisionWriter(dsn, role string) error {
	return provision(dsn, role, []string{},
		`GRANT USAGE ON SCHEMA public TO %s`,
		`GRANT INSERT (event_type, content, refs) ON public.events TO %s`,
		`GRANT SELECT ON public.events TO %s`,
	)
}

// ProvisionReader grants an existing login role SELECT on the canonical
// event table and nothing else. Run with an administrator/provisioning
// DSN.
func ProvisionReader(dsn, role string) error {
	// A reader must not be able to append at all, so INSERT on any column is
	// disqualifying too.
	return provision(dsn, role, []string{"INSERT"},
		`GRANT USAGE ON SCHEMA public TO %s`,
		`GRANT SELECT ON public.events TO %s`,
	)
}

// quoteIdentifier validates and safely quotes a PostgreSQL role name for
// interpolation into GRANT statements, which cannot parameterize
// identifiers the way DML parameterizes values.
func quoteIdentifier(role string) (string, error) {
	if role == "" {
		return "", fmt.Errorf("postgres: role name is required")
	}
	return pgx.Identifier{role}.Sanitize(), nil
}

// Append performs one direct database transaction: PostgreSQL assigns id,
// received_at, and actor, validates event_type/content/refs itself, and
// commits. Append returns the complete stored event only after that commit
// is confirmed by the server. Any other outcome is an error: a plain
// validation error if n failed Duro's client-side pre-check (never
// attempted against the database), or an *AppendError classifying whether
// the failure is a confirmed non-commit or an unknown outcome from a lost
// connection. Append never retries; resubmission is an explicit caller
// action and creates a new event.
func (s *Store) Append(n event.New) (StoredEvent, error) {
	if err := n.Validate(); err != nil {
		return StoredEvent{}, err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return StoredEvent{}, &AppendError{Outcome: OutcomeNotCommitted, Err: err}
	}
	defer tx.Rollback()

	var se StoredEvent
	var content, refs []byte
	err = tx.QueryRow(
		`INSERT INTO public.events (event_type, content, refs) VALUES ($1, $2, $3)
		 RETURNING id, received_at, event_type, actor, content, refs`,
		n.EventType, []byte(n.ContentOrDefault()), []byte(n.RefsOrDefault()),
	).Scan(&se.ID, &se.ReceivedAt, &se.EventType, &se.Actor, &content, &refs)
	if err != nil {
		// Whatever this error's cause, PostgreSQL never received (or never
		// accepted) a COMMIT for this transaction: it is definitely not
		// committed.
		return StoredEvent{}, &AppendError{Outcome: OutcomeNotCommitted, Err: err}
	}
	se.Content = json.RawMessage(content)
	se.Refs = json.RawMessage(refs)

	if err := tx.Commit(); err != nil {
		return StoredEvent{}, &AppendError{Outcome: classifyCommitErr(err), Err: err}
	}
	return se, nil
}

// ErrEventNotFound is returned by Get when no event exists with the
// requested id.
var ErrEventNotFound = errors.New("postgres: event not found")

// Get retrieves one event by id, exactly as PostgreSQL stored it: a single
// schema-qualified, parameterized SELECT of all six columns from
// public.events. It never writes. id must be a canonical hyphenated UUIDv7
// (see event.ValidID, the same validation the CLI applies before opening a
// connection); Get validates it again itself so a malformed or malicious id
// never reaches the database regardless of caller.
func (s *Store) Get(id string) (StoredEvent, error) {
	if !event.ValidID(id) {
		return StoredEvent{}, fmt.Errorf("postgres: %q is not a canonical UUIDv7 event id", id)
	}
	var se StoredEvent
	var content, refs []byte
	err := s.db.QueryRow(
		`SELECT id, received_at, event_type, actor, content, refs FROM public.events WHERE id = $1`,
		id,
	).Scan(&se.ID, &se.ReceivedAt, &se.EventType, &se.Actor, &content, &refs)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredEvent{}, ErrEventNotFound
	}
	if err != nil {
		return StoredEvent{}, err
	}
	se.Content = json.RawMessage(content)
	se.Refs = json.RawMessage(refs)
	return se, nil
}

// classifyCommitErr distinguishes a server-confirmed non-commit from an
// outcome Duro cannot confirm. pgx.ErrTxCommitRollback means the server
// answered that the commit rolled back, and sql.ErrTxDone means database/sql
// never sent this COMMIT at all: both are definite non-commits. An ordinary
// *pgconn.PgError (ERROR severity: constraint violation, a deferred
// constraint failing at COMMIT, a rejected statement) is PostgreSQL's own
// answer that this transaction did not commit.
//
// Not every server error is that evidence. A FATAL or PANIC severity, a
// connection-exception SQLSTATE (class 08), or an administrator/crash
// shutdown (57P01, 57P02, 57P03) reports that the session is being torn down
// -- not that this transaction rolled back rather than committed just before
// the session died. Those are classified unknown, the same as an error with
// no server answer at all (connection reset, timeout): PostgreSQL may have
// committed before the acknowledgement was lost. Duro never automatically
// resubmits an unknown outcome; retrying is an explicit caller action and
// creates a new event.
func classifyCommitErr(err error) CommitOutcome {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case pgErr.Severity == "FATAL", pgErr.Severity == "PANIC",
			strings.HasPrefix(pgErr.Code, "08"),
			pgErr.Code == "57P01", pgErr.Code == "57P02", pgErr.Code == "57P03":
			return OutcomeUnknown
		}
		return OutcomeNotCommitted
	}
	if errors.Is(err, pgx.ErrTxCommitRollback) || errors.Is(err, sql.ErrTxDone) {
		return OutcomeNotCommitted
	}
	return OutcomeUnknown
}
