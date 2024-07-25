PRAGMA disable_object_cache;

SET autoinstall_known_extensions=1;

SET autoload_known_extensions=1;

SET memory_limit='1333MB';

SET threads = 2;

SET preserve_insertion_order=false;

PRAGMA temp_directory='/tmp/';

COPY (
		SELECT id AS "id", (updated_at::TIMESTAMPTZ) AS "updated_at", log AS "log", CAST(test_decimal AS decimal(38,19)) AS "test_decimal", updated_date AS "updated_date" FROM read_parquet(['0.parquet','1.parquet','2.parquet','3.parquet'])

) TO 'serialized_file.parquet' (FORMAT 'PARQUET', CODEC 'SNAPPY');

SELECT COUNT(*)
FROM (
	SELECT
	id AS "id", updated_at AS "updated_at", log AS "log", CAST(test_decimal AS VARCHAR) AS "test_decimal", updated_date AS "updated_date"
	FROM
	    read_parquet('serialized_file.parquet')
);


