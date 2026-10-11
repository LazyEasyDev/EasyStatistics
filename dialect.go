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
	tableName       = "easy_statistics"
	batchTableName  = "easy_statistics_batches"
	expiryIndexName = "easy_statistics_expire_idx"
	gaussEmptyValue = "!"
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

func (dialect SQLDialect) bindKey(index, length int) string {
	parameter := dialect.bind(index)
	if dialect == SQLServer {
		return fmt.Sprintf("CAST(%s AS VARCHAR(%d))", parameter, length)
	}
	return parameter
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
	for index, name := range names {
		parameters[index] = dialect.bind(index + 1)
		if dialect == SQLGaussDB && name == "dimension_value" {
			parameters[index] = "COALESCE(NULLIF(" + parameters[index] + ", ''), '" + gaussEmptyValue + "')"
		}
	}
	return "INSERT INTO " + dialect.quote(table) + " (" + dialect.columns(names...) + ") VALUES (" + strings.Join(parameters, ", ") + ")"
}

func (dialect SQLDialect) selectRecord() string {
	columns := dialect.columns("name", "dimension_id", "dimension_fields", "dimension_value", "group_interval", "grouped_time", "last_update_time", "counter")
	if dialect == SQLOracle {
		// A driver may represent both SQL NULL and EMPTY_CLOB() as nil. Keep
		// the actual SQL nullness available so corrupt nullable schemas fail.
		columns += ", CASE WHEN " + dialect.quote("dimension_value") + " IS NULL THEN 1 ELSE 0 END"
	}
	return "SELECT " + columns + " FROM " + dialect.quote(tableName) + " WHERE " + dialect.quote("record_id") + " = " + dialect.bindKey(1, 64)
}

func (dialect SQLDialect) selectBatch() string {
	return "SELECT " + dialect.quote("batch_id") + " FROM " + dialect.quote(batchTableName) + " WHERE " + dialect.quote("batch_id") + " = " + dialect.bindKey(1, 96)
}

func (dialect SQLDialect) insertBatch() string {
	return dialect.insert(batchTableName, "batch_id", "created_unix_time")
}

func (dialect SQLDialect) deleteBatch() string {
	return "DELETE FROM " + dialect.quote(batchTableName) + " WHERE " + dialect.quote("batch_id") + " = " + dialect.bindKey(1, 96)
}

func (dialect SQLDialect) pruneBatches() string {
	return "DELETE FROM " + dialect.quote(batchTableName) + " WHERE " + dialect.quote("created_unix_time") + " < " + dialect.bind(1)
}

func (dialect SQLDialect) pruneRecords() string {
	table := dialect.quote(tableName)
	expiry := dialect.quote("expire_time")
	identity := dialect.quote("record_id")
	cutoff := dialect.bind(1)
	if dialect == SQLSQLite {
		cutoff = "?1"
	}
	predicate := expiry + " > 0 AND " + expiry + " <= " + cutoff
	query := "DELETE FROM " + table + " WHERE " + predicate
	switch dialect {
	case SQLMySQL, SQLMariaDB, SQLTiDB:
		return query + " ORDER BY " + expiry + fmt.Sprintf(" LIMIT %d", cleanupBatchSize)
	case SQLServer:
		return fmt.Sprintf("DELETE TOP (%d) FROM ", cleanupBatchSize) + table + " WHERE " + predicate
	case SQLOracle:
		return query + fmt.Sprintf(" AND ROWNUM <= %d", cleanupBatchSize)
	case SQLPostgreSQL, SQLGaussDB, SQLSQLite:
		return query + " AND " + identity + " IN (SELECT " + identity + " FROM " + table + " WHERE " + predicate +
			" ORDER BY " + expiry + fmt.Sprintf(" LIMIT %d)", cleanupBatchSize)
	default:
		return ""
	}
}

func (dialect SQLDialect) createExpiryIndex() string {
	return "CREATE INDEX " + dialect.quote(expiryIndexName) + " ON " + dialect.quote(tableName) + " (" + dialect.quote("expire_time") + ")"
}

func (dialect SQLDialect) expiryIndexQuery() string {
	switch dialect {
	case SQLMySQL, SQLMariaDB, SQLTiDB:
		return `SELECT COUNT(*) FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'easy_statistics'
AND COLUMN_NAME = 'expire_time' AND SEQ_IN_INDEX = 1 AND NON_UNIQUE = 1 AND INDEX_TYPE = 'BTREE'`
	case SQLPostgreSQL, SQLGaussDB:
		return `SELECT COUNT(*) FROM pg_index AS indexes
JOIN pg_class AS relations ON relations.oid = indexes.indexrelid
JOIN pg_am AS methods ON methods.oid = relations.relam
JOIN pg_attribute AS fields ON fields.attrelid = indexes.indrelid AND fields.attnum = indexes.indkey[0]
WHERE indexes.indrelid = 'easy_statistics'::regclass AND fields.attname = 'expire_time'
AND NOT indexes.indisunique AND indexes.indisvalid AND indexes.indpred IS NULL
AND methods.amname IN ('btree', 'ubtree')`
	case SQLSQLite:
		return `SELECT COUNT(*) FROM pragma_index_list('easy_statistics') AS indexes
JOIN pragma_index_info(indexes.name) AS fields ON fields.seqno = 0
WHERE fields.name = 'expire_time' AND indexes."unique" = 0 AND indexes.partial = 0`
	case SQLServer:
		return `SELECT COUNT(*) FROM sys.indexes AS indexes
JOIN sys.index_columns AS keys ON keys.object_id = indexes.object_id AND keys.index_id = indexes.index_id
JOIN sys.columns AS fields ON fields.object_id = keys.object_id AND fields.column_id = keys.column_id
WHERE indexes.object_id = OBJECT_ID(N'easy_statistics', N'U') AND fields.name = N'expire_time'
AND keys.key_ordinal = 1 AND indexes.is_unique = 0 AND indexes.is_disabled = 0
AND indexes.is_hypothetical = 0 AND indexes.has_filter = 0 AND indexes.type IN (1, 2)`
	case SQLOracle:
		return `SELECT COUNT(*) FROM USER_INDEXES indexes
JOIN USER_IND_COLUMNS fields ON fields.INDEX_NAME = indexes.INDEX_NAME AND fields.TABLE_NAME = indexes.TABLE_NAME
WHERE indexes.TABLE_NAME = 'easy_statistics' AND fields.COLUMN_NAME = 'expire_time'
AND fields.COLUMN_POSITION = 1 AND indexes.UNIQUENESS = 'NONUNIQUE'
AND indexes.STATUS = 'VALID' AND indexes.INDEX_TYPE = 'NORMAL'`
	default:
		return ""
	}
}

func (dialect SQLDialect) insertRecord() string {
	names := []string{"record_id", "name", "dimension_id", "dimension_fields", "dimension_value", "group_interval", "grouped_time", "last_update_time", "expire_time", "counter"}
	if dialect == SQLOracle {
		return "INSERT INTO " + dialect.quote(tableName) + " (" + dialect.columns(names...) + ") VALUES (:1, :2, :3, NVL(TO_CLOB(:4), EMPTY_CLOB()), NVL(TO_CLOB(:5), EMPTY_CLOB()), :6, :7, :8, :9, :10)"
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
			if name == "record_id" {
				parameter = dialect.bindKey(index+1, 64)
			}
			if dialect == SQLOracle && (name == "dimension_fields" || name == "dimension_value") {
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

func (dialect SQLDialect) startOracleClob(column string) string {
	quoted := dialect.quote(column)
	return "UPDATE " + dialect.quote(tableName) + " SET " + quoted + " = TO_CLOB(:1) WHERE " +
		dialect.quote("record_id") + " = :2 AND DBMS_LOB.COMPARE(" + quoted + ", TO_CLOB(:3)) = 0"
}

func (dialect SQLDialect) appendOracleClob(column string) string {
	quoted := dialect.quote(column)
	return "UPDATE " + dialect.quote(tableName) + " SET " + quoted + " = " + quoted + " || TO_CLOB(:1) WHERE " +
		dialect.quote("record_id") + " = :2 AND DBMS_LOB.GETLENGTH(" + quoted + ") = :3"
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
