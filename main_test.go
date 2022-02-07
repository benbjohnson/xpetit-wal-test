package main_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestWAL(t *testing.T) {
	for _, n := range []int{1000, 10000, 100000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			testWAL(t, n)
		})
	}
}

func testWAL(t *testing.T, n int) {
	dir := t.TempDir()

	db, err := sql.Open("sqlite3", filepath.Join(dir, "db")+"?_busy_timeout=5000&_journal_mode=wal&_synchronous=normal")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// db.SetMaxOpenConns(1) // this fixes the issue

	if _, err := db.Exec(`create table if not exists "tb" (
		"id"    integer not null primary key,
		"count" integer not null
	)`); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < n; i++ {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		result, err := tx.Exec(`update "tb" set "count" = "count" + 1 where "id" = 1`)
		if err != nil {
			t.Fatal(err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			t.Fatal(err)
		} else if n == 0 {
			if _, err := tx.Exec(`insert into "tb" ("id", "count") values (1, 1)`); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Read database contents.
	rows, err := db.Query(`SELECT id, count FROM tb`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, count int
		if err := rows.Scan(&id, &count); err != nil {
			t.Fatal(err)
		}
		t.Logf("row: id=%d count=%d", id, count)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify DB size
	fi, err := os.Stat(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("db size:  %d bytes", fi.Size())

	// Verify WAL size
	fi, err = os.Stat(filepath.Join(dir, "db-wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("wal size: %d bytes", fi.Size())
}
