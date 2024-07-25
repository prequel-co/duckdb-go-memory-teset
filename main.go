package main 

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/marcboeker/go-duckdb"
)

const CompactionQuery = `

COPY (
		SELECT id AS "id", (updated_at::TIMESTAMPTZ) AS "updated_at", log AS "log", CAST(test_decimal AS decimal(38,19)) AS "test_decimal", updated_date AS "updated_date" FROM read_parquet(['0.parquet','1.parquet','2.parquet','3.parquet'])

) TO 'serialized_file.parquet' (FORMAT 'PARQUET', CODEC 'SNAPPY')

`

const ReadQuery = `
SELECT
	id AS "id", updated_at AS "updated_at", log AS "log", CAST(test_decimal AS VARCHAR) AS "test_decimal", updated_date AS "updated_date"
FROM
    read_parquet('serialized_file.parquet')
`

func main() {
	db, err := openDuckDB("test.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec(CompactionQuery)
	if err != nil {
		panic(err)
	}

	rows, err := db.Queryx(ReadQuery)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	count := 0

	for rows.Next(){
		count += 1
	}

	if rows.Err() != nil {
		panic(rows.Err())
	}	


	fmt.Printf("Found: %d rows\n", count)

}

func openDuckDB(dbName string) (*sqlx.DB, error) {
	duckDb, err := sqlx.Open("duckdb", dbName)
	if err != nil {
		return nil, fmt.Errorf("error opening connection to duckdb: %w", err)
	}

	_, err = duckDb.Exec("PRAGMA disable_object_cache;")
	if err != nil {
		return nil, fmt.Errorf("error disabling object cache: %w", err)
	}

	_, err = duckDb.Exec("SET autoinstall_known_extensions=1;")
	if err != nil {
		return nil, fmt.Errorf("error installing json extension: %w", err)
	}

	_, err = duckDb.Exec("SET autoload_known_extensions=1;")
	if err != nil {
		return nil, fmt.Errorf("error loading json extension: %w", err)
	}

	_, err = duckDb.Exec("SET memory_limit='1333MB'")
	if err != nil {
		return nil, fmt.Errorf("error setting DuckDB memory limit: %w", err)
	}

	_, err = duckDb.Exec("SET threads = 2;")
	if err != nil {
		return nil, fmt.Errorf("error setting DuckDB thread count: %w", err)
	}

	_, err = duckDb.Exec("SET preserve_insertion_order=false;")
	if err != nil {
		return nil, fmt.Errorf("error setting DuckDB insertion order: %w", err)
	}

	_, err = duckDb.Exec("PRAGMA temp_directory='/tmp/';")
	if err != nil {
		return nil, fmt.Errorf("error setting temp_directory: %w", err)
	}

	return duckDb, nil
}