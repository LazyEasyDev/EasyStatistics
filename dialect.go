package EasyStatistics

import (
	"fmt"
	"strings"
)

type SQLDialect string

const (
	SQLPostgreSQL SQLDialect = "postgresql"
	SQLMySQL      SQLDialect = "mysql"
	SQLMariaDB    SQLDialect = "mariadb"
	SQLTiDB       SQLDialect = "tidb"
	SQLSQLite     SQLDialect = "sqlite"
	SQLServer     SQLDialect = "sqlserver"
	SQLGaussDB    SQLDialect = "gaussdb"
	SQLOracle     SQLDialect = "oracle"
)

const (
	tableName      = "easy_statistics"
	batchTableName = "easy_statistics_batches"
)

func schemaFor(dialect SQLDialect) ([]string, error) {
	switch dialect {
	case SQLPostgreSQL, SQLGaussDB:
		return []string{
			`CREATE TABLE IF NOT EXISTS "easy_statistics" (
"record_id" VARCHAR(64) PRIMARY KEY,
"name" TEXT NOT NULL,
"dimension_id" VARCHAR(64) NOT NULL,
"dimension_fields" TEXT NOT NULL,
"dimension_value" TEXT NOT NULL,
"group_interval" VARCHAR(8) NOT NULL,
"grouped_time" BIGINT NOT NULL,
"last_update_time" BIGINT NOT NULL,
"counter" BIGINT NOT NULL
)`,
			`CREATE TABLE IF NOT EXISTS "easy_statistics_batches" ("batch_id" VARCHAR(96) PRIMARY KEY, "created_unix_time" BIGINT NOT NULL)`,
		}, nil
	case SQLMySQL, SQLMariaDB, SQLTiDB:
		return []string{
			"CREATE TABLE IF NOT EXISTS `easy_statistics` (" +
				"`record_id` VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY," +
				"`name` VARCHAR(255) NOT NULL," +
				"`dimension_id` VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL," +
				"`dimension_fields` LONGTEXT NOT NULL," +
				"`dimension_value` LONGTEXT NOT NULL," +
				"`group_interval` VARCHAR(8) NOT NULL," +
				"`grouped_time` BIGINT NOT NULL," +
				"`last_update_time` BIGINT NOT NULL," +
				"`counter` BIGINT NOT NULL" +
				") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
			"CREATE TABLE IF NOT EXISTS `easy_statistics_batches` (`batch_id` VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY, `created_unix_time` BIGINT NOT NULL) ENGINE=InnoDB",
		}, nil
	case SQLSQLite:
		return []string{
			`CREATE TABLE IF NOT EXISTS "easy_statistics" (
"record_id" TEXT PRIMARY KEY,
"name" TEXT NOT NULL,
"dimension_id" TEXT NOT NULL,
"dimension_fields" TEXT NOT NULL,
"dimension_value" TEXT NOT NULL,
"group_interval" TEXT NOT NULL,
"grouped_time" INTEGER NOT NULL,
"last_update_time" INTEGER NOT NULL,
"counter" INTEGER NOT NULL
) WITHOUT ROWID`,
			`CREATE TABLE IF NOT EXISTS "easy_statistics_batches" ("batch_id" TEXT PRIMARY KEY, "created_unix_time" INTEGER NOT NULL) WITHOUT ROWID`,
		}, nil
	case SQLServer:
		return []string{
			sqlServerCreate(tableName, `[record_id] VARCHAR(64) NOT NULL PRIMARY KEY,
[name] NVARCHAR(255) NOT NULL,
[dimension_id] VARCHAR(64) NOT NULL,
[dimension_fields] NVARCHAR(MAX) NOT NULL,
[dimension_value] NVARCHAR(MAX) NOT NULL,
[group_interval] VARCHAR(8) NOT NULL,
[grouped_time] BIGINT NOT NULL,
[last_update_time] BIGINT NOT NULL,
[counter] BIGINT NOT NULL`),
			sqlServerCreate(batchTableName, `[batch_id] VARCHAR(96) NOT NULL PRIMARY KEY, [created_unix_time] BIGINT NOT NULL`),
		}, nil
	case SQLOracle:
		return []string{
			oracleCreate(tableName, `"record_id" VARCHAR2(64) PRIMARY KEY,
"name" VARCHAR2(255 CHAR) NOT NULL,
"dimension_id" VARCHAR2(64) NOT NULL,
"dimension_fields" CLOB NOT NULL,
"dimension_value" CLOB NOT NULL,
"group_interval" VARCHAR2(8) NOT NULL,
"grouped_time" NUMBER(19) NOT NULL,
"last_update_time" NUMBER(19) NOT NULL,
"counter" NUMBER(19) NOT NULL`),
			oracleCreate(batchTableName, `"batch_id" VARCHAR2(96) PRIMARY KEY, "created_unix_time" NUMBER(19) NOT NULL`),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported SQL dialect %q", dialect)
	}
}

func sqlServerCreate(table, columns string) string {
	return fmt.Sprintf(`BEGIN TRY
IF OBJECT_ID(N'%s', N'U') IS NULL
    CREATE TABLE [%s] (%s)
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() <> 2714 OR OBJECT_ID(N'%s', N'U') IS NULL
        THROW;
END CATCH`, table, table, columns, table)
}

func oracleCreate(table, columns string) string {
	return fmt.Sprintf(`DECLARE table_count PLS_INTEGER;
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE "%s" (%s)';
EXCEPTION WHEN OTHERS THEN
    IF SQLCODE != -955 THEN RAISE; END IF;
    SELECT COUNT(*) INTO table_count FROM USER_TABLES WHERE TABLE_NAME = '%s';
    IF table_count = 0 THEN RAISE; END IF;
END;`, table, columns, table)
}

func (backend *sqlBackend) quote(identifier string) string {
	switch backend.dialect {
	case SQLMySQL, SQLMariaDB, SQLTiDB:
		return "`" + identifier + "`"
	case SQLServer:
		return "[" + identifier + "]"
	default:
		return `"` + identifier + `"`
	}
}

func (backend *sqlBackend) bind(index int) string {
	switch backend.dialect {
	case SQLPostgreSQL, SQLGaussDB:
		return fmt.Sprintf("$%d", index)
	case SQLServer:
		return fmt.Sprintf("@p%d", index)
	case SQLOracle:
		return fmt.Sprintf(":%d", index)
	default:
		return "?"
	}
}

func (backend *sqlBackend) columns(names ...string) string {
	quoted := make([]string, len(names))
	for index, name := range names {
		quoted[index] = backend.quote(name)
	}
	return strings.Join(quoted, ", ")
}

func (backend *sqlBackend) insert(table string, names ...string) string {
	parameters := make([]string, len(names))
	for index := range names {
		parameters[index] = backend.bind(index + 1)
	}
	return "INSERT INTO " + backend.quote(table) + " (" + backend.columns(names...) + ") VALUES (" + strings.Join(parameters, ", ") + ")"
}

func (backend *sqlBackend) selectRecord(locked bool) string {
	query := "SELECT " + backend.columns("name", "dimension_id", "dimension_fields", "dimension_value", "group_interval", "grouped_time", "last_update_time", "counter") + " FROM " + backend.quote(tableName)
	if locked && backend.dialect == SQLServer {
		query += " WITH (UPDLOCK, HOLDLOCK)"
	}
	query += " WHERE " + backend.quote("record_id") + " = " + backend.bind(1)
	if locked && backend.dialect != SQLSQLite && backend.dialect != SQLServer {
		query += " FOR UPDATE"
	}
	return query
}

func (backend *sqlBackend) selectBatch() string {
	return "SELECT " + backend.quote("batch_id") + " FROM " + backend.quote(batchTableName) + " WHERE " + backend.quote("batch_id") + " = " + backend.bind(1)
}

func (backend *sqlBackend) currentUnixTime() string {
	switch backend.dialect {
	case SQLPostgreSQL, SQLGaussDB:
		return "CAST(FLOOR(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)) AS BIGINT)"
	case SQLMySQL, SQLMariaDB, SQLTiDB:
		return "UNIX_TIMESTAMP()"
	case SQLSQLite:
		return "CAST(strftime('%s', 'now') AS INTEGER)"
	case SQLServer:
		return "DATEDIFF_BIG(SECOND, CAST('1970-01-01T00:00:00' AS DATETIME2), SYSUTCDATETIME())"
	default:
		return "FLOOR((CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE) - DATE '1970-01-01') * 86400)"
	}
}

func (backend *sqlBackend) insertBatch() string {
	return "INSERT INTO " + backend.quote(batchTableName) + " (" + backend.columns("batch_id", "created_unix_time") + ") VALUES (" + backend.bind(1) + ", " + backend.currentUnixTime() + ")"
}

func (backend *sqlBackend) deleteBatch() string {
	return "DELETE FROM " + backend.quote(batchTableName) + " WHERE " + backend.quote("batch_id") + " = " + backend.bind(1)
}

func (backend *sqlBackend) pruneBatches() string {
	return "DELETE FROM " + backend.quote(batchTableName) + " WHERE " + backend.quote("created_unix_time") + " < " + backend.currentUnixTime() + " - " + backend.bind(1)
}

func (backend *sqlBackend) pruneRecords() string {
	return "DELETE FROM " + backend.quote(tableName) + " WHERE " + backend.quote("dimension_id") + " = " + backend.bind(1) + " AND " + backend.quote("last_update_time") + " < " + backend.currentUnixTime() + " - " + backend.bind(2)
}

func (backend *sqlBackend) insertRecord() string {
	return backend.insert(tableName, "record_id", "name", "dimension_id", "dimension_fields", "dimension_value", "group_interval", "grouped_time", "last_update_time", "counter")
}

func (backend *sqlBackend) updateRecord() string {
	return "UPDATE " + backend.quote(tableName) + " SET " + backend.quote("counter") + " = " + backend.bind(1) + ", " + backend.quote("last_update_time") + " = " + backend.bind(2) + " WHERE " + backend.quote("record_id") + " = " + backend.bind(3)
}

func (backend *sqlBackend) schemaQuery(index int) string {
	table := tableName
	names := []string{"record_id", "name", "dimension_id", "dimension_fields", "dimension_value", "group_interval", "grouped_time", "last_update_time", "counter"}
	if index == 1 {
		table = batchTableName
		names = []string{"batch_id", "created_unix_time"}
	}
	columns := make([]string, len(names))
	for position, name := range names {
		columns[position] = backend.quote(table) + "." + backend.quote(name)
	}
	return "SELECT " + strings.Join(columns, ", ") + " FROM " + backend.quote(table) + " WHERE 1 = 0"
}
