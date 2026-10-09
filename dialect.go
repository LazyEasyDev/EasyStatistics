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
"expire_time" BIGINT NOT NULL,
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
				"`expire_time` BIGINT NOT NULL," +
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
"expire_time" INTEGER NOT NULL,
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
[expire_time] BIGINT NOT NULL,
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
"expire_time" NUMBER(19) NOT NULL,
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

func (dialect SQLDialect) quote(identifier string) string {
	switch dialect {
	case SQLMySQL, SQLMariaDB, SQLTiDB:
		return "`" + identifier + "`"
	case SQLServer:
		return "[" + identifier + "]"
	default:
		return `"` + identifier + `"`
	}
}

func (dialect SQLDialect) bind(index int) string {
	switch dialect {
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

func (dialect SQLDialect) columns(names ...string) string {
	quoted := make([]string, len(names))
	for index, name := range names {
		quoted[index] = dialect.quote(name)
	}
	return strings.Join(quoted, ", ")
}

func (dialect SQLDialect) insert(table string, names ...string) string {
	parameters := make([]string, len(names))
	for index := range names {
		parameters[index] = dialect.bind(index + 1)
	}
	return "INSERT INTO " + dialect.quote(table) + " (" + dialect.columns(names...) + ") VALUES (" + strings.Join(parameters, ", ") + ")"
}

func (dialect SQLDialect) selectRecord() string {
	return "SELECT " + dialect.columns("name", "dimension_id", "dimension_fields", "dimension_value", "group_interval", "grouped_time", "last_update_time", "counter") + " FROM " + dialect.quote(tableName) + " WHERE " + dialect.quote("record_id") + " = " + dialect.bind(1)
}

func (dialect SQLDialect) selectBatch() string {
	return "SELECT " + dialect.quote("batch_id") + " FROM " + dialect.quote(batchTableName) + " WHERE " + dialect.quote("batch_id") + " = " + dialect.bind(1)
}

func (dialect SQLDialect) currentUnixTime() string {
	switch dialect {
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

func (dialect SQLDialect) insertBatch() string {
	return "INSERT INTO " + dialect.quote(batchTableName) + " (" + dialect.columns("batch_id", "created_unix_time") + ") VALUES (" + dialect.bind(1) + ", " + dialect.currentUnixTime() + ")"
}

func (dialect SQLDialect) deleteBatch() string {
	return "DELETE FROM " + dialect.quote(batchTableName) + " WHERE " + dialect.quote("batch_id") + " = " + dialect.bind(1)
}

func (dialect SQLDialect) pruneBatches() string {
	return "DELETE FROM " + dialect.quote(batchTableName) + " WHERE " + dialect.quote("created_unix_time") + " < " + dialect.currentUnixTime() + " - " + dialect.bind(1)
}

func (dialect SQLDialect) pruneRecords() string {
	return "DELETE FROM " + dialect.quote(tableName) + " WHERE " + dialect.quote("expire_time") + " > 0 AND " + dialect.quote("expire_time") + " <= " + dialect.currentUnixTime()
}

func (dialect SQLDialect) insertRecord() string {
	names := []string{"record_id", "name", "dimension_id", "dimension_fields", "dimension_value", "group_interval", "grouped_time", "last_update_time", "expire_time", "counter"}
	if dialect == SQLOracle {
		return "INSERT INTO " + dialect.quote(tableName) + " (" + dialect.columns(names...) + ") VALUES (:1, :2, :3, :4, NVL(TO_CLOB(:5), EMPTY_CLOB()), :6, :7, :8, :9, :10)"
	}
	return dialect.insert(tableName, names...)
}

func (dialect SQLDialect) upsertRecord() (string, error) {
	counter := dialect.quote("counter")
	updated := dialect.quote("last_update_time")
	switch dialect {
	case SQLMySQL, SQLMariaDB, SQLTiDB:
		updates := dialect.upsertAssignments(counter, "VALUES("+counter+")", updated, "VALUES("+updated+")", dialect.bind(11))
		return dialect.insertRecord() + " ON DUPLICATE KEY UPDATE " + updates, nil
	case SQLGaussDB:
		updates := dialect.upsertAssignments(counter, "EXCLUDED."+counter, updated, "EXCLUDED."+updated, dialect.bind(11))
		return dialect.insertRecord() + " ON DUPLICATE KEY UPDATE " + updates, nil
	case SQLPostgreSQL, SQLSQLite:
		table := dialect.quote(tableName) + "."
		updates := dialect.upsertAssignments(table+counter, "excluded."+counter, table+updated, "excluded."+updated, dialect.bind(11))
		return dialect.insertRecord() + " ON CONFLICT (" + dialect.quote("record_id") + ") DO UPDATE SET " + updates, nil
	case SQLServer, SQLOracle:
		names := []string{"record_id", "name", "dimension_id", "dimension_fields", "dimension_value", "group_interval", "grouped_time", "last_update_time", "expire_time", "counter"}
		sources := make([]string, len(names)+1)
		values := make([]string, len(names))
		for index, name := range names {
			parameter := dialect.bind(index + 1)
			if dialect == SQLOracle && name == "dimension_value" {
				parameter = "NVL(TO_CLOB(" + parameter + "), EMPTY_CLOB())"
			}
			sources[index] = parameter + " AS " + dialect.quote(name)
			values[index] = "incoming." + dialect.quote(name)
		}
		sources[len(names)] = dialect.bind(11) + " AS " + dialect.quote("clear_after")
		source := "SELECT " + strings.Join(sources, ", ")
		table := dialect.quote(tableName)
		if dialect == SQLOracle {
			source += " FROM DUAL"
		} else {
			table += " WITH (HOLDLOCK)"
		}
		identity := dialect.quote("record_id")
		updates := dialect.upsertAssignments("target."+counter, "incoming."+counter, "target."+updated, "incoming."+updated, "incoming."+dialect.quote("clear_after"))
		query := "MERGE INTO " + table + " target USING (" + source + ") incoming ON (target." + identity + " = incoming." + identity + ")" +
			" WHEN MATCHED THEN UPDATE SET " + updates +
			" WHEN NOT MATCHED THEN INSERT (" + dialect.columns(names...) + ") VALUES (" + strings.Join(values, ", ") + ")"
		if dialect == SQLServer {
			query += ";"
		}
		return query, nil
	default:
		return "", fmt.Errorf("unsupported SQL dialect %q", dialect)
	}
}

func (dialect SQLDialect) upsertAssignments(counter, delta, updated, added, clearAfter string) string {
	if dialect == SQLPostgreSQL || dialect == SQLGaussDB {
		clearAfter = "CAST(" + clearAfter + " AS BIGINT)"
	}
	latest := "CASE WHEN " + updated + " > " + added + " THEN " + updated + " ELSE " + added + " END"
	return dialect.quote("counter") + " = " + counterAddition(counter, delta) + ", " +
		dialect.quote("last_update_time") + " = " + latest + ", " +
		dialect.quote("expire_time") + " = COALESCE((" + latest + ") + NULLIF(" + clearAfter + ", 0), 0)"
}

func counterAddition(counter, delta string) string {
	const maximum = "9223372036854775807"
	const minimum = "-9223372036854775808"
	sum := counter + " + " + delta
	return "CASE WHEN " + delta + " > 0 THEN CASE WHEN " + counter + " > " + maximum + " - " + delta +
		" THEN (" + counter + " - " + maximum + ") + (" + delta + " - " + maximum + ") - 2 ELSE " + sum + " END " +
		"WHEN " + delta + " < 0 THEN CASE WHEN " + counter + " < " + minimum + " - " + delta +
		" THEN (" + counter + " + " + maximum + " + 1) + (" + delta + " + " + maximum + " + 1) ELSE " + sum + " END " +
		"ELSE " + counter + " END"
}

func (dialect SQLDialect) schemaQuery(index int) string {
	table := tableName
	names := []string{"record_id", "name", "dimension_id", "dimension_fields", "dimension_value", "group_interval", "grouped_time", "last_update_time", "expire_time", "counter"}
	if index == 1 {
		table = batchTableName
		names = []string{"batch_id", "created_unix_time"}
	}
	columns := make([]string, len(names))
	for position, name := range names {
		columns[position] = dialect.quote(table) + "." + dialect.quote(name)
	}
	return "SELECT " + strings.Join(columns, ", ") + " FROM " + dialect.quote(table) + " WHERE 1 = 0"
}
