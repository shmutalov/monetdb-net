using System.Collections.Generic;
using System.Data.MonetDb.Enums;
using System.Data.MonetDb.Extensions;
using System.Data.MonetDb.Models;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using MonetDB.Driver.Models;

namespace System.Data.MonetDb.Helpers
{
    internal class MonetDbMetaData : IDisposable
    {
        /// <summary>
        /// Parent connection
        /// </summary>
        private readonly MonetDbConnection _connection;

        /// <summary>
        /// Synchronization object
        /// </summary>
        private readonly object _lock = new object();

        public MonetDbConnection Connection
        {
            get { return _connection; }
        }

        /// <summary>
        /// Driver version
        /// </summary>
        private string _driverVersion;

        /// <summary>
        /// Environment variables for each connection
        /// </summary>
        private static IDictionary<MonetDbConnection, IDictionary<string, string>> _environmentVariables;

        static MonetDbMetaData()
        {
            _environmentVariables = new Dictionary<MonetDbConnection, IDictionary<string, string>>();
        }

        public MonetDbMetaData(MonetDbConnection connection)
        {
            _connection = connection;
        }

        private MonetDbCommand GetCommand()
        {
            lock (_lock)
            {
                return _connection.CreateCommand() as MonetDbCommand;
            }
        }

        /// <summary>
        /// Internal cache for environment properties retrieved from the
        /// server. To avoid querying the server over and over again, once a
        /// value is read, it is kept in a Dictionary for reuse.
        /// </summary>
        /// <param name="keyName">Environment variable name</param>
        /// <returns>
        /// Returns environment variable value if set, or <code>string.Empty</code>
        /// </returns>
        public string GetEnvironmentVariable(string keyName)
        {
            lock (_lock)
            {
                var envs = new Dictionary<string, string>();

                var command = GetCommand();
                command.CommandText = @"SELECT env.name
                                            , env.value 
                                        FROM sys.env() as env";

                try
                {
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var varName = reader["name"].To<string>();
                            var varValue = reader["value"].To<string>();

                            envs.Add(varName, varValue);
                        }
                    }
                }
                catch (Exception)
                {
                    // ignore 
                }

                // add variables to cache
                _environmentVariables[_connection] = envs;

                // TODO: check session variables

                return envs.ContainsKey(keyName) ? envs[keyName] : string.Empty;
            }
        }

        /// <summary>
        /// Can all the procedures returned 
        /// by <see cref="GetProcedures"/> be called by the current user?
        /// </summary>
        /// <remarks>
        /// Always <code>true</code>
        /// </remarks>
        public bool AllProceduresCallable
        {
            get { return true; }
        }

        /// <summary>
        /// Can all the tables returned by getTable be SELECTed by the current user?
        /// </summary>
        /// <remarks>
        /// Always <code>true</code> because we only have one user a.t.m.
        /// </remarks>
        public bool AllTablesSelectable
        {
            get { return true; }
        }

        /// <summary>
        /// What is the ConnectionString for this database?
        /// </summary>
        public string ConnectionString
        {
            get { return _connection.ConnectionString; }
        }

        /// <summary>
        /// What is our user name as known to the database?
        /// </summary>
        public string UserName
        {
            get { return GetEnvironmentVariable("current_user"); }
        }

        /// <summary>
        /// Is the database in read-only mode?
        /// </summary>
        /// <remarks>
        /// Always <code>false</code> for now
        /// </remarks>
        public bool ReadOnly
        {
            get { return false; }
        }

        /// <summary>
        /// Are NULL values sorted high?
        /// </summary>
        /// <remarks>
        /// Always <code>true</code> because MonetDB puts NULL values on top upon ORDER BY
        /// </remarks>
        public bool NullsSortedHigh
        {
            get { return true; }
        }

        /// <summary>
        /// Are NULL values sorted low?
        /// </summary>
        /// <remarks>
        /// Always <code>false</code> because MonetDB puts NULL values on top upon ORDER BY.
        /// </remarks>
        /// <seealso cref="NullsSortedHigh"/>
        public bool NullsSortedLow
        {
            get { return !NullsSortedHigh; }
        }

        /// <summary>
        /// Are NULL values sorted at the start regardless of sort order?
        /// </summary>
        /// <remarks>
        /// Always <code>false</code>, since MonetDB doesn't do this
        /// </remarks>
        public bool NullsSortedAtStart
        {
            get { return false; }
        }

        /// <summary>
        /// Are NULL values sorted at the end regardless of sort order?
        /// </summary>
        /// <remarks>
        /// Always <code>false</code>, since MonetDB doesn't do this
        /// </remarks>
        public bool NullsSortedAtEnd
        {
            get { return false; }
        }

        /// <summary>
        /// What is the name of this database product - this should be MonetDB
        /// of course, so we return that explicitly.
        /// </summary>
        public string DatabaseProductName
        {
            get { return "MonetDB"; }
        }

        /// <summary>
        /// What is the version of this database product. 
        /// Returns a fixed version number, yes it's quick and dirty
        /// </summary>
        public string DatabaseProductVersion
        {
            get { return GetEnvironmentVariable("monet_version"); }
        }

        /// <summary>
        /// What is the name of this JDBC driver?  If we don't know this
        /// we are doing something wrong!
        /// </summary>
        public string DriverName
        {
            get { return "DataMicorn MonetDB .NET Driver"; }
        }

        /// <summary>
        /// MonetDB driver version
        /// </summary>
        public string DriverVersion
        {
            get
            {
                if (string.IsNullOrEmpty(_driverVersion))
                {
                    var assembly = Assembly.GetExecutingAssembly();
                    var info = FileVersionInfo.GetVersionInfo(assembly.Location);
                    _driverVersion = info.FileVersion;
                }

                return _driverVersion;
            }
        }

        /// <summary>
        /// Get a comma separated list of all a database's SQL keywords that
        /// are NOT also SQL:2003 keywords.
        /// </summary>
        public string SqlKeywords
        {
            get
            {
                return "ADMIN,AFTER,AGGREGATE,ALWAYS,ASYMMETRIC,ATOMIC," +
                       "AUTO_INCREMENT,BEFORE,BIGINT,BIGSERIAL,BINARY,BLOB," +
                       "CALL,CHAIN,CLOB,COMMITTED,COPY,CORR,CUME_DIST," +
                       "CURRENT_ROLE,CYCLE,DATABASE,DELIMITERS,DENSE_RANK," +
                       "DO,EACH,ELSEIF,ENCRYPTED,EVERY,EXCLUDE,FOLLOWING," +
                       "FUNCTION,GENERATED,IF,ILIKE,INCREMENT,LAG,LEAD," +
                       "LIMIT,LOCALTIME,LOCALTIMESTAMP,LOCKED,MAXVALUE," +
                       "MEDIAN,MEDIUMINT,MERGE,MINVALUE,NEW,NOCYCLE," +
                       "NOMAXVALUE,NOMINVALUE,NOW,OFFSET,OLD,OTHERS,OVER," +
                       "PARTITION,PERCENT_RANK,PLAN,PRECEDING,PROD,QUANTILE," +
                       "RANGE,RANK,RECORDS,REFERENCING,REMOTE,RENAME," +
                       "REPEATABLE,REPLICA,RESTART,RETURN,RETURNS," +
                       "ROW_NUMBER,ROWS,SAMPLE,SAVEPOINT,SCHEMA,SEQUENCE," +
                       "SERIAL,SERIALIZABLE,SIMPLE,START,STATEMENT,STDIN," +
                       "STDOUT,STREAM,STRING,SYMMETRIC,TIES,TINYINT,TRIGGER," +
                       "UNBOUNDED,UNCOMMITTED,UNENCRYPTED,WHILE,XMLAGG," +
                       "XMLATTRIBUTES,XMLCOMMENT,XMLCONCAT,XMLDOCUMENT," +
                       "XMLELEMENT,XMLFOREST,XMLNAMESPACES,XMLPARSE,XMLPI," +
                       "XMLQUERY,XMLSCHEMA,XMLTEXT,XMLVALIDATE";
            }
        }

        private IList<MonetDbFunctionInfoModel> GetFunctions(MonetDbFunctionType kind)
        {
            var functions = new List<MonetDbFunctionInfoModel>();

            // where clause part (for num/str/timedate to match only functions whose 1 arg exists and is of a certain type
            var part1 = "WHERE \"id\" IN (SELECT \"func_id\" FROM \"sys\".\"args\" WHERE \"number\" = 1 AND \"name\" = 'arg_1' AND \"type\" IN ";
            string whereClause;

            switch (kind)
            {
                case MonetDbFunctionType.Numeric:
                    whereClause = part1 +
                        "('tinyint', 'smallint', 'int', 'bigint', 'decimal', 'real', 'double') )" +
                        // exclude 2 functions which take an int as arg but returns a char or str
                        " AND \"name\" NOT IN ('code', 'space')";
                    break;
                case MonetDbFunctionType.String:
                    whereClause = part1 +
                        "('char', 'varchar', 'clob') )" +
                        // include 2 functions which take an int as arg but returns a char or str
                        " OR \"name\" IN ('code', 'space')";
                    break;
                case MonetDbFunctionType.System:
                    whereClause = "WHERE \"id\" NOT IN (SELECT \"func_id\" FROM \"sys\".\"args\" WHERE \"number\" = 1)" +
                        " AND \"func\" NOT LIKE '%function%(% %)%'" +
                        " AND \"func\" NOT LIKE '%procedure%(% %)%'" +
                        " AND \"func\" NOT LIKE '%CREATE FUNCTION%RETURNS TABLE(% %)%'" +
                        // the next names are also not usable so exclude them
                        " AND \"name\" NOT LIKE 'querylog_%'" +
                        " AND \"name\" NOT IN ('analyze', 'count', 'count_no_nil', 'initializedictionary', 'times')";
                    break;
                case MonetDbFunctionType.DateTime:
                    whereClause = part1 +
                    "('date', 'time', 'timestamp', 'timetz', 'timestamptz', 'sec_interval', 'month_interval') )";
                    break;
                default:
                    return functions;
            }

            var command = GetCommand();

            try
            {
                var selectQuery = "SELECT DISTINCT \"name\" FROM \"sys\".\"functions\" " + whereClause + " ORDER BY 1";
                command.CommandText = selectQuery;

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        functions.Add(new MonetDbFunctionInfoModel
                        {
                            Name = reader.GetString(reader.GetOrdinal("name"))
                        });
                    }
                }
            }
            catch (Exception)
            {
                // ignore
            }

            return functions;
        }

        public IList<MonetDbFunctionInfoModel> GetNumericFunctions()
        {
            return GetFunctions(MonetDbFunctionType.Numeric);
        }

        public IList<MonetDbFunctionInfoModel> GetStringFunctions()
        {
            return GetFunctions(MonetDbFunctionType.String);
        }

        public IList<MonetDbFunctionInfoModel> GetSystemFunctions()
        {
            return GetFunctions(MonetDbFunctionType.System);
        }

        public IList<MonetDbFunctionInfoModel> GetDateTimeFunctions()
        {
            return GetFunctions(MonetDbFunctionType.DateTime);
        }

        /// <summary>
        /// This is the string that can be used to escape '_' and '%' in
        /// a search string pattern style catalog search parameters
        /// </summary>
        public string SearchStringEscape
        {
            get { return "\\"; }
        }

        /// <summary>
        /// Get all the "extra" characters that can be used in unquoted
        /// identifier names(those beyond a-zA-Z0-9 and _)
        /// MonetDB has no extras
        /// </summary>
        public IList<char> ExtraCharacters
        {
            get { return "".ToList(); }
        }

        /// <summary>
        /// Is "ALTER TABLE" with an add column supported?
        /// </summary>
        public bool SupportsAlterTableAddColumn
        {
            get { return true; }
        }

        /// <summary>
        /// Is "ALTER TABLE" with a drop column supported?
        /// </summary>
        public bool SupportsAlterTableDropColumn
        {
            get { return true; }
        }

        /// <summary>
        /// Is column aliasing supported?
        /// </summary>
        public bool SupportsColumnAlias
        {
            get { return true; }
        }

        /// <summary>
        /// How many active connections can we have at a time to this database?
        /// </summary>
        public int MaxConnections
        {
            get { return GetEnvironmentVariable("max_clients").To<int>(16); }
        }

        /// <summary>
        /// What is the database's default transaction isolation level?
        /// </summary>
        public IsolationLevel DefaultTransactionIsolation
        {
            get { return IsolationLevel.Serializable; }
        }

        /// <summary>
        /// Are transactions supported?	If not, commit and rollback are noops
        /// and the isolation level is TRANSACTION_NONE.  We do support
        /// transactions.
        /// </summary>
        public bool SupportTransactions
        {
            get { return true; }
        }

        /// <summary>
        /// Does the database support the given transaction isolation level?
        /// </summary>
        /// <param name="level">Isolation level</param>
        /// <returns>
        /// <code>true</code> if supported
        /// </returns>
        public bool IsTransactionIsolationLevelSupported(IsolationLevel level)
        {
            return level == IsolationLevel.Serializable;
        }

        /// <summary>
        /// Get a description of stored procedures available in a catalog.
        /// Currently not applicable and not implemented, returns null
        /// </summary>
        /// <param name="catalog"></param>
        /// <param name="schemaPattern"></param>
        /// <param name="procedureNamePattern"></param>
        /// <returns></returns>
        public IList<MonetDbProcedureInfoModel> GetProcedures(string catalog
            , string schemaPattern
            , string procedureNamePattern)
        {
            var procedures = new List<MonetDbProcedureInfoModel>();

            var command = GetCommand();
            command.CommandText = @"SELECT cast(null AS varchar(1)) AS ""PROCEDURE_CAT""
                                        , cast(null AS varchar(1)) AS ""PROCEDURE_SCHEM""
                                        , '' AS ""PROCEDURE_NAME""
                                        , cast(null AS varchar(1)) AS ""FIELD4""
                                        , cast(null AS varchar(1)) AS ""FIELD5""
                                        , cast(null AS varchar(1)) AS ""FIELD6""
                                        , '' AS ""REMARKS""
                                        , cast(0 AS smallint) AS ""PROCEDURE_TYPE""
                                    WHERE 1 = 0";

            try
            {
                using (var reader = command.ExecuteReader())
                {
                    procedures.Add(new MonetDbProcedureInfoModel
                    {
                        Catalog = reader.GetString(reader.GetOrdinal("PROCEDURE_CAT")),
                        Schema = reader.GetString(reader.GetOrdinal("PROCEDURE_SCHEM")),
                        Name = reader.GetString(reader.GetOrdinal("PROCEDURE_NAME")),
                        Field4 = reader.GetString(reader.GetOrdinal("FIELD4")),
                        Field5 = reader.GetString(reader.GetOrdinal("FIELD5")),
                        Filed6 = reader.GetString(reader.GetOrdinal("FIELD6")),
                        Remarks = reader.GetString(reader.GetOrdinal("REMARKS")),
                        Type = reader.GetInt16(reader.GetOrdinal("PROCEDURE_TYPE")),
                    });
                }
            }
            catch (Exception)
            {
                // ignore
            }

            return procedures;
        }

        /// <summary>
        /// Returns the given string where all slashes and single quotes are
        /// escaped with a slash.
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        private static string EscapeQuotes(string input)
        {
            return input.Replace("\\\\", "\\\\\\\\").Replace("'", "\\\\'");
        }

        /// <summary>
        /// Get a description of tables available in a catalog.
        /// </summary>
        /// <param name="catalog">A catalog name. This parameter is currently ignored</param>
        /// <param name="schemaPattern">A schema name pattern</param>
        /// <param name="tableNamePattern">A table name pattern. For all tables this should be "%"</param>
        /// <param name="types">a list of table types to include. Null returns all types. 
        /// This parameter is currently ignored</param>
        /// <returns></returns>
        public IList<MonetDbTableInfoModel> GetTables(
            string catalog,
            string schemaPattern,
            string tableNamePattern,
            params string[] types)
        {
            var tables = new List<MonetDbTableInfoModel>();

            var cat = GetEnvironmentVariable("gdk_dbname");

            // as of Jul2015 release the sys.tables.type values (0 through 6) is extended with new values 10, 11, 20, and 30 (for system and temp tables/views).
            // for correct behavior we need to know if the server is using the old (pre Jul2015) or new sys.tables.type values
            var preJul2015 = (string.Compare("11.19.15", DatabaseProductVersion, StringComparison.InvariantCultureIgnoreCase) >= 0);

            var selectQuery =
                "SELECT * FROM ( " +
                "SELECT '" + cat + "' AS \"TABLE_CAT\", \"schemas\".\"name\" AS \"TABLE_SCHEM\", \"tables\".\"name\" AS \"TABLE_NAME\", " +
                    "CASE WHEN \"tables\".\"system\" = true AND \"tables\".\"type\" = " + (preJul2015 ? "0" : "10") + " AND \"tables\".\"temporary\" = 0 THEN 'SYSTEM TABLE' " +
                    "WHEN \"tables\".\"system\" = true AND \"tables\".\"type\" = " + (preJul2015 ? "1" : "11") + " AND \"tables\".\"temporary\" = 0 THEN 'SYSTEM VIEW' " +
                    "WHEN \"tables\".\"system\" = false AND \"tables\".\"type\" = 0 AND \"tables\".\"temporary\" = 0 THEN 'TABLE' " +
                    "WHEN \"tables\".\"system\" = false AND \"tables\".\"type\" = 1 AND \"tables\".\"temporary\" = 0 THEN 'VIEW' " +
                    "WHEN \"tables\".\"system\" = true AND \"tables\".\"type\" = " + (preJul2015 ? "0" : "20") + " AND \"tables\".\"temporary\" = 1 THEN 'SYSTEM SESSION TABLE' " +
                    "WHEN \"tables\".\"system\" = true AND \"tables\".\"type\" = " + (preJul2015 ? "1" : "21") + " AND \"tables\".\"temporary\" = 1 THEN 'SYSTEM SESSION VIEW' " +
                    "WHEN \"tables\".\"system\" = false AND \"tables\".\"type\" = " + (preJul2015 ? "0" : "30") + " AND \"tables\".\"temporary\" = 1 THEN 'SESSION TABLE' " +
                    "WHEN \"tables\".\"system\" = false AND \"tables\".\"type\" = " + (preJul2015 ? "1" : "31") + " AND \"tables\".\"temporary\" = 1 THEN 'SESSION VIEW' " +
                    "END AS \"TABLE_TYPE\", \"tables\".\"query\" AS \"REMARKS\", null AS \"TYPE_CAT\", null AS \"TYPE_SCHEM\", " +
                    "null AS \"TYPE_NAME\", 'rowid' AS \"SELF_REFERENCING_COL_NAME\", 'SYSTEM' AS \"REF_GENERATION\" " +
                "FROM \"sys\".\"tables\" AS \"tables\", \"sys\".\"schemas\" AS \"schemas\" WHERE \"tables\".\"schema_id\" = \"schemas\".\"id\" " +
                ") AS \"tables\" WHERE 1 = 1 ";

            if (!string.IsNullOrEmpty(tableNamePattern))
            {
                selectQuery += "AND LOWER(\"TABLE_NAME\") LIKE '" + EscapeQuotes(tableNamePattern).ToLower() + "' ";
            }

            if (!string.IsNullOrEmpty(tableNamePattern))
            {
                selectQuery += "AND LOWER(\"TABLE_SCHEM\") LIKE '" + EscapeQuotes(schemaPattern).ToLower() + "' ";
            }

            if (types != null && types.Length > 0)
            {
                selectQuery += "AND (";
                for (var i = 0; i < types.Length; i++)
                {
                    selectQuery += (i == 0 ? "" : " OR ") + "LOWER(\"TABLE_TYPE\") LIKE '" + EscapeQuotes(types[i]).ToLower() + "'";
                }
                selectQuery += ") ";
            }

            var orderByClause = "ORDER BY \"TABLE_TYPE\", \"TABLE_SCHEM\", \"TABLE_NAME\" ";

            var command = GetCommand();
            command.CommandText = selectQuery + orderByClause;

            try
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        tables.Add(new MonetDbTableInfoModel
                        {
                            Catalog = reader.GetString(reader.GetOrdinal("TABLE_CAT")),
                            Schema = reader.GetString(reader.GetOrdinal("TABLE_SCHEM")),
                            Name = reader.GetString(reader.GetOrdinal("TABLE_NAME")),
                            Type = reader.GetString(reader.GetOrdinal("TABLE_TYPE")),
                            Remarks = reader.GetString(reader.GetOrdinal("REMARKS")),
                        });
                    }
                }
            }
            catch (Exception)
            {
                // ignore
            }

            return tables;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="catalog"></param>
        /// <param name="schemaPattern"></param>
        /// <returns></returns>
        public IList<MonetDbSchemaInfoModel> GetSchemas(
            string catalog,
            string schemaPattern)
        {
            var schemas = new List<MonetDbSchemaInfoModel>();

            var cat = GetEnvironmentVariable("gdk_dbname");
            var query = "SELECT \"name\" AS \"TABLE_SCHEM\", " +
                            "'" + cat + "' AS \"TABLE_CATALOG\", " +
                            "'" + cat + "' AS \"TABLE_CAT\" " + // SquirrelSQL requests this one...
                        "FROM \"sys\".\"schemas\" " +
                        "WHERE 1 = 1 ";

            if (!string.IsNullOrEmpty(catalog))
                query += "AND LOWER('" + cat + "') LIKE '" + EscapeQuotes(catalog).ToLower() + "' ";

            if (!string.IsNullOrEmpty(schemaPattern))
                query += "AND LOWER(\"name\") LIKE '" + EscapeQuotes(schemaPattern).ToLower() + "' ";

            query += "ORDER BY \"TABLE_SCHEM\"";

            var command = GetCommand();
            command.CommandText = query;

            try
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        schemas.Add(new MonetDbSchemaInfoModel
                        {
                            Catalog = reader.GetString(reader.GetOrdinal("TABLE_CATALOG")),
                            Name = reader.GetString(reader.GetOrdinal("TABLE_SCHEM")),
                        });
                    }
                }
            }
            catch (Exception)
            {
                // ignore
            }

            return schemas;
        }

        /// <summary>
        /// Get the catalog names available in this database. 
        /// The results are ordered by catalog name.
        /// </summary>
        /// <returns></returns>
        public IList<MonetDbCatalogInfoModel> GetCatalogs()
        {
            var catalogs = new List<MonetDbCatalogInfoModel>();

            var cat = GetEnvironmentVariable("gdk_dbname");

            catalogs.Add(new MonetDbCatalogInfoModel
            {
                Name = cat,
            });

            return catalogs;
        }

        /// <summary>
        /// Get the table types available in this database.	The results
        /// are ordered by table type.
        /// </summary>
        /// <returns></returns>
        public IList<MonetDbTableTypeInfoModel> GetTableTypes()
        {
            // The results need to be ordered by TABLE_TYPE
            var types = new List<MonetDbTableTypeInfoModel>
            {
                new MonetDbTableTypeInfoModel {Name = "SESSION TABLE"},
                new MonetDbTableTypeInfoModel {Name = "SESSION VIEW"},
                new MonetDbTableTypeInfoModel {Name = "SYSTEM SESSION TABLE"},
                new MonetDbTableTypeInfoModel {Name = "SYSTEM SESSION VIEW"},
                new MonetDbTableTypeInfoModel {Name = "SYSTEM TABLE"},
                new MonetDbTableTypeInfoModel {Name = "SYSTEM VIEW"},
                new MonetDbTableTypeInfoModel {Name = "TABLE"},
                new MonetDbTableTypeInfoModel {Name = "VIEW"}
            };

            return types;
        }

        /// <summary>
        /// Get a description of table columns available in a catalog.
        /// </summary>
        /// <param name="catalog">a catalog name; "" retrieves those without a catalog. Currently ignored</param>
        /// <param name="schemaNamePattern">a schema name pattern; "" retrieves those without a schema</param>
        /// <param name="tableNamePattern">a table name pattern</param>
        /// <param name="columnNamePattern">a column name pattern</param>
        /// <returns></returns>
        public IList<MonetDbColumnInfoModel> GetColumns(
            string catalog,
            string schemaNamePattern,
            string tableNamePattern,
            string columnNamePattern)
        {
            var columns = new List<MonetDbColumnInfoModel>();
            var cat = GetEnvironmentVariable("gdk_dbname");

            var query =
                "SELECT '" + cat + "' AS \"TABLE_CAT\", \"schemas\".\"name\" AS \"TABLE_SCHEM\", " +
                "\"tables\".\"name\" AS \"TABLE_NAME\", \"columns\".\"name\" AS \"COLUMN_NAME\", " +
                "\"columns\".\"type\" AS \"TYPE_NAME\", " +
                "\"columns\".\"type_digits\" AS \"COLUMN_SIZE\", " +
                "0 AS \"BUFFER_LENGTH\", \"columns\".\"type_scale\" AS \"DECIMAL_DIGITS\", " +
                "10 AS \"NUM_PREC_RADIX\", " +
                "cast(CASE \"null\" " +
                    "WHEN true THEN 1 " +
                    "WHEN false THEN 0 " +
                "END AS int) AS \"NULLABLE\", cast(null AS varchar(1)) AS \"REMARKS\", " +
                "\"columns\".\"default\" AS \"COLUMN_DEF\", " +
                "0 AS \"CHAR_OCTET_LENGTH\", " +
                "\"columns\".\"number\" + 1 AS \"ORDINAL_POSITION\" " +
                    "FROM \"sys\".\"columns\" AS \"columns\", " +
                        "\"sys\".\"tables\" AS \"tables\", " +
                        "\"sys\".\"schemas\" AS \"schemas\" " +
                    "WHERE \"columns\".\"table_id\" = \"tables\".\"id\" " +
                        "AND \"tables\".\"schema_id\" = \"schemas\".\"id\" ";

            if (!string.IsNullOrEmpty(schemaNamePattern))
            {
                query += "AND LOWER(\"schemas\".\"name\") LIKE '" + EscapeQuotes(schemaNamePattern).ToLower() + "' ";
            }
            if (!string.IsNullOrEmpty(tableNamePattern))
            {
                query += "AND LOWER(\"tables\".\"name\") LIKE '" + EscapeQuotes(tableNamePattern).ToLower() + "' ";
            }
            if (!string.IsNullOrEmpty(columnNamePattern))
            {
                query += "AND LOWER(\"columns\".\"name\") LIKE '" + EscapeQuotes(columnNamePattern).ToLower() + "' ";
            }

            query += "ORDER BY \"TABLE_SCHEM\", \"TABLE_NAME\", \"ORDINAL_POSITION\"";

            var command = GetCommand();
            command.CommandText = query;

            try
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        columns.Add(new MonetDbColumnInfoModel
                        {
                            Catalog = reader["TABLE_CAT"].To<string>(),
                            Schema = reader["TABLE_SCHEM"].To<string>(),
                            Table = reader["TABLE_NAME"].To<string>(),
                            Name = reader["COLUMN_NAME"].To<string>(),
                            DataType = reader["TYPE_NAME"].To<string>(),
                            ColumnSize = reader["COLUMN_SIZE"].To<int>(),
                            Scale = reader["DECIMAL_DIGITS"].To<int>(),
                            Radix = reader["NUM_PREC_RADIX"].To<int>(),
                            Nullable = reader["NULLABLE"].To<int>() == 1,
                            Remarks = reader["REMARKS"].To<string>(),
                            DefaultValue = reader["COLUMN_DEF"].To<string>(),
                            CharOctetLength = reader["CHAR_OCTET_LENGTH"].To<int>(),
                            Ordinal = reader["ORDINAL_POSITION"].To<int>(),
                        });
                    }
                }
            }
            catch (Exception)
            {
                // ignore
            }

            return columns;
        }

        public bool IsColumnUniqueKey(
                string catalog,
                string schemaName,
                string tableName,
                string columnName)
        {
            var cat = GetEnvironmentVariable("gdk_dbname");
            var query = "SELECT 1 " +
                        "FROM " +
                            "\"sys\".\"keys\" AS \"keys\", " +
                            "\"sys\".\"objects\" AS \"objects\", " +
                            "\"sys\".\"tables\" AS \"tables\", " +
                            "\"sys\".\"schemas\" AS \"schemas\" " +
                        "WHERE \"keys\".\"id\" = \"objects\".\"id\" " +
                            "AND \"keys\".\"table_id\" = \"tables\".\"id\" " +
                            "AND \"tables\".\"schema_id\" = \"schemas\".\"id\" " +
                            "AND \"keys\".\"type\" = 1 "; // 1 = unique key

            if (string.IsNullOrEmpty(schemaName))
                schemaName = "sys";

            query += "AND LOWER(\"schemas\".\"name\") = '" + EscapeQuotes(schemaName).ToLower() + "' ";

            if (string.IsNullOrEmpty(tableName))
                return false;

            query += "AND LOWER(\"tables\".\"name\") = '" + EscapeQuotes(tableName).ToLower() + "' ";

            if (string.IsNullOrEmpty(columnName))
                return false;

            query += "AND LOWER(\"objects\".\"name\") = '" + EscapeQuotes(columnName).ToLower() + "' ";
            query += "LIMIT 1";

            var command = GetCommand();
            command.CommandText = query;

            try
            {
                var result = command.ExecuteScalar();
                return result != null && result.To<int>() == 1;
            }
            catch (Exception)
            {
                // ignore
            }

            return false;
        }

        public bool IsColumnPrimaryKey(
                string catalog,
                string schemaName,
                string tableName,
                string columnName)
        {
            var cat = GetEnvironmentVariable("gdk_dbname");
            var query = "SELECT 1 " +
                        "FROM " +
                            "\"sys\".\"keys\" AS \"keys\", " +
                            "\"sys\".\"objects\" AS \"objects\", " +
                            "\"sys\".\"tables\" AS \"tables\", " +
                            "\"sys\".\"schemas\" AS \"schemas\" " +
                        "WHERE \"keys\".\"id\" = \"objects\".\"id\" " +
                            "AND \"keys\".\"table_id\" = \"tables\".\"id\" " +
                            "AND \"tables\".\"schema_id\" = \"schemas\".\"id\" " +
                            "AND \"keys\".\"type\" = 0 "; // 1 = primary key

            if (string.IsNullOrEmpty(schemaName))
                schemaName = "sys";

            query += "AND LOWER(\"schemas\".\"name\") = '" + EscapeQuotes(schemaName).ToLower() + "' ";

            if (string.IsNullOrEmpty(tableName))
                return false;

            query += "AND LOWER(\"tables\".\"name\") = '" + EscapeQuotes(tableName).ToLower() + "' ";

            if (string.IsNullOrEmpty(columnName))
                return false;

            query += "AND LOWER(\"objects\".\"name\") = '" + EscapeQuotes(columnName).ToLower() + "' ";
            query += "LIMIT 1";

            var command = GetCommand();
            command.CommandText = query;

            try
            {
                var result = command.ExecuteScalar();
                return result != null && result.To<int>() == 1;
            }
            catch (Exception)
            {
                // ignore
            }

            return false;
        }

        public void Dispose()
        {
            
        }
    }
}
