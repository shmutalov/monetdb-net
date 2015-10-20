using System.Collections.Generic;
using System.Data.MonetDb.Constants;
using System.Data.MonetDb.Extensions;
using System.Data.MonetDb.Helpers;
using System.Data.MonetDb.Helpers.Mapi;
using System.Data.MonetDb.Models;
using System.Linq;

namespace System.Data.MonetDb
{
    /// <summary>
    /// Provides a means of reading one or more forward-only streams of result sets obtained by executing a command on a MonetDB server
    /// </summary>
    public class MonetDbDataReader : IDataReader
    {
        private bool _isOpen = true;
        private MonetDbQueryResponseInfo _responseInfo;
        private IEnumerable<MonetDbQueryResponseInfo> _responseInfoEnumerable;
        private readonly IEnumerator<MonetDbQueryResponseInfo> _responeInfoEnumerator;
        private IEnumerator<List<string>> _enumerator;
        private readonly MonetDbConnection _connection;

        private MonetDbMetaData _metaData;
        private DataTable _schemaTable;

        internal MonetDbDataReader(IEnumerable<MonetDbQueryResponseInfo> ri, MonetDbConnection connection)
        {
            _metaData = null;
            _schemaTable = null;

            _connection = connection;
            _responseInfoEnumerable = ri;
            _responeInfoEnumerator = ri.GetEnumerator();
            NextResult();
        }

        private DataTable GenerateSchemaTable()
        {
            if (_metaData == null)
                _metaData = new MonetDbMetaData(_connection);

            // schema table always must be named as "SchemaTable"
            var table = new DataTable("SchemaTable");

            // create table schema columns
            table.Columns.Add(MonetDbSchemaTableColumns.ColumnName, typeof(string));
            table.Columns.Add(MonetDbSchemaTableColumns.ColumnOrdinal, typeof(int));
            table.Columns.Add(MonetDbSchemaTableColumns.ColumnSize, typeof(int));
            table.Columns.Add(MonetDbSchemaTableColumns.NumericPrecision, typeof(int));
            table.Columns.Add(MonetDbSchemaTableColumns.NumericScale, typeof(int));
            table.Columns.Add(MonetDbSchemaTableColumns.DataType, typeof(Type));
            table.Columns.Add(MonetDbSchemaTableColumns.ProviderType, typeof(int));
            table.Columns.Add(MonetDbSchemaTableColumns.ProviderSpecificDataType, typeof(DbType));
            table.Columns.Add(MonetDbSchemaTableColumns.IsLong, typeof(bool));
            table.Columns.Add(MonetDbSchemaTableColumns.AllowDbNull, typeof(bool));
            table.Columns.Add(MonetDbSchemaTableColumns.IsReadOnly, typeof(bool));
            table.Columns.Add(MonetDbSchemaTableColumns.IsRowVersion, typeof(bool));
            table.Columns.Add(MonetDbSchemaTableColumns.IsUnique, typeof(bool));
            table.Columns.Add(MonetDbSchemaTableColumns.IsKey, typeof(bool));
            table.Columns.Add(MonetDbSchemaTableColumns.IsAutoincrement, typeof(bool));
            table.Columns.Add(MonetDbSchemaTableColumns.BaseSchemaName, typeof(string));
            table.Columns.Add(MonetDbSchemaTableColumns.BaseCatalogName, typeof(string));
            table.Columns.Add(MonetDbSchemaTableColumns.BaseTableName, typeof(string));
            table.Columns.Add(MonetDbSchemaTableColumns.BaseColumnName, typeof(string));
            table.Columns.Add(MonetDbSchemaTableColumns.DataTypeName, typeof(string));

            // fill table
            for (var fieldIndex = 0; fieldIndex < FieldCount; fieldIndex++)
            {
                // get column name
                var columnName = GetName(fieldIndex);

                // get column size
                var columnSize = _responseInfo.Columns[fieldIndex].Length;

                //// get column precision
                var numericPrecision = 0;
                //DieQueryError();

                //// get column scale
                var numericScale = 0;
                //DieQueryError();

                // get data type
                var providerType = _responseInfo.Columns[fieldIndex].DataType;

                var dbType = providerType.GetDbType();
                var providerSpecificDataType = dbType;
                var systemType = providerType.GetSystemType();

                // is binary large object
                var lobs = new[] { "blob" };
                var isLong = lobs.Contains(providerType);

                // is nullable
                // TODO: retreive information from sys.columns table
                var allowDbNull = true;

                // is read only
                // MonetDB does not support cursor updates, so
                // nothing is writable.
                var isReadOnly = true;

                // is rowid
                var isRowVersion = false;

                // is unique
                var isUnique = false;

                // is key
                var isKey = false;

                // is nullable
                var isAutoincrement = providerType.Equals("oid");

                // get column base table name. Result contains both schema and table name
                // so, we need split them
                var baseFullTableName = _responseInfo.Columns[fieldIndex].TableName;

                var baseTableName = baseFullTableName;
                var baseSchemaName = string.Empty;

                if (baseFullTableName.IndexOf("."
                    , StringComparison.InvariantCultureIgnoreCase) != -1)
                {
                    var s = baseFullTableName.Split('.');

                    // get column base schema name
                    baseSchemaName = s[0];
                    baseTableName = s[1];
                }

                // get column meta data
                var columnsInfo = _metaData.GetColumns("", "", baseTableName, columnName);
                MonetDbColumnInfoModel columnInfo;

                if (columnsInfo.Count == 1)
                {
                    columnInfo = columnsInfo[0];
                }
                else
                {
                    columnInfo = new MonetDbColumnInfoModel
                    {
                        Catalog = _metaData.GetEnvironmentVariable("gdk_dbname"),
                        CharOctetLength = 0,
                        ColumnSize = columnSize,
                        DataType = providerType,
                        DefaultValue = null,
                        Name = string.Empty,
                        Nullable = allowDbNull, // Actually, we don't know about this
                        Ordinal = fieldIndex, // In schema table we must always use results field index
                        Radix = 10,
                        Remarks = string.Empty,
                        Scale = numericScale,
                        Schema = baseSchemaName,
                        Table = baseTableName
                    };
                }

                // get additional info
                isUnique = _metaData.IsColumnUniqueKey("", columnInfo.Schema,
                    columnInfo.Table, columnInfo.Name);
                isKey = _metaData.IsColumnPrimaryKey("", columnInfo.Schema,
                        columnInfo.Table, columnInfo.Name);

                table.Rows.Add(
                        columnName,
                        fieldIndex,
                        columnSize,
                        numericPrecision,
                        numericScale,
                        systemType,
                        providerSpecificDataType.To<int>(),
                        providerSpecificDataType,
                        isLong,
                        columnInfo.Nullable,
                        isReadOnly,
                        isRowVersion,
                        isUnique,
                        isKey,
                        isAutoincrement,
                        columnInfo.Schema,
                        columnInfo.Catalog,
                        columnInfo.Table,
                        columnInfo.Name,
                        providerType
                    );
            }

            return table;
        }

        #region IDataReader Members

        /// <summary>
        /// Closes the IDataReader object
        /// </summary>
        public void Close()
        {
            Dispose();
        }

        /// <summary>
        /// Gets a value indicating the depth of nesting for the current row.
        /// </summary>
        public int Depth
        {
            get { throw new Exception("The method or operation is not implemented."); }
        }

        /// <summary>
        /// Returns a <c>DataTable</c> that describes the column metadata of the <c>IDataReader</c>.
        /// </summary>
        /// <returns></returns>
        public DataTable GetSchemaTable()
        {
            return _schemaTable??(_schemaTable = GenerateSchemaTable());
        }

        /// <summary>
        /// Gets a value indicating whether the data reader is closed.
        /// </summary>
        public bool IsClosed
        {
            get { return _isOpen == false; }
        }

        /// <summary>
        /// Advances the data reader to the next result, when reading the results of batch SQL statements.
        /// </summary>
        /// <returns></returns>
        public bool NextResult()
        {
            var flag = _responeInfoEnumerator.MoveNext();
            _responseInfo = _responeInfoEnumerator.Current;
            _enumerator = _responseInfo.Data.GetEnumerator();

            // we need to regenerate schema table for next result set
            if (_schemaTable != null)
            {
                _schemaTable = GenerateSchemaTable();
            }

            return flag;
        }

        /// <summary>
        /// Advances the <c>IDataReader</c> to the next record.
        /// </summary>
        /// <returns></returns>
        public bool Read()
        {
            return _enumerator.MoveNext();
        }

        /// <summary>
        /// Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.
        /// </summary>
        public int RecordsAffected
        {
            get { return _responseInfo.RecordsAffected; }
        }

        #endregion

        #region IDisposable Members

        /// <summary>
        /// Closes the data reader
        /// </summary>
        public void Dispose()
        {
            while (_responeInfoEnumerator.MoveNext())
            {
                while (_enumerator.MoveNext())
                {
                }
                _enumerator.Dispose();
            }
            _responeInfoEnumerator.Dispose();
        }

        #endregion

        #region IDataRecord Members

        /// <summary>
        /// Gets the number of columns in the current row.
        /// </summary>
        public int FieldCount
        {
            get { return _responseInfo.ColumnCount; }
        }

        /// <summary>
        /// Gets the value of the specified column as a Boolean.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public bool GetBoolean(int i)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        /// <summary>
        /// Gets the 8-bit unsigned integer value of the specified column.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public byte GetByte(int i)
        {
            return (byte)GetInt16(i);
        }

        /// <summary>
        /// Reads a stream of bytes from the specified column offset into the buffer as an array, starting at the given buffer offset.
        /// </summary>
        /// <param name="i"></param>
        /// <param name="fieldOffset"></param>
        /// <param name="buffer"></param>
        /// <param name="bufferoffset"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        /// <summary>
        /// Gets the character value of the specified column.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public char GetChar(int i)
        {
            return _enumerator.Current[i][0];
        }

        /// <summary>
        /// Reads a stream of characters from the specified column offset into the buffer as an array, starting at the given buffer offset.
        /// </summary>
        /// <param name="i"></param>
        /// <param name="fieldoffset"></param>
        /// <param name="buffer"></param>
        /// <param name="bufferoffset"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        /// <summary>
        /// Gets an <c>IDataReader</c> to be used when the field points to more remote structured data.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public IDataReader GetData(int i)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        /// <summary>
        /// Gets the data type information for the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public string GetDataTypeName(int i)
        {
            return _responseInfo.Columns[i].DataType;
        }

        /// <summary>
        /// Gets the date and time data value of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public DateTime GetDateTime(int i)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        /// <summary>
        /// Gets the fixed-position numeric value of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public decimal GetDecimal(int i)
        {
            return decimal.Parse(_enumerator.Current[i]);
        }

        /// <summary>
        /// Gets the double-precision floating point number of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public double GetDouble(int i)
        {
            return double.Parse(_enumerator.Current[i]);
        }

        /// <summary>
        /// Gets the <c>Type</c> information corresponding to the type of <c>Object</c> that would be returned from <c>GetValue</c>.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public Type GetFieldType(int i)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        /// <summary>
        /// Gets the single-precision floating point number of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public float GetFloat(int i)
        {
            return float.Parse(_enumerator.Current[i]);
        }

        /// <summary>
        /// Returns the GUID value of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public Guid GetGuid(int i)
        {
            return new Guid(_enumerator.Current[i]);
        }

        /// <summary>
        /// Gets the 16-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public short GetInt16(int i)
        {
            return short.Parse(_enumerator.Current[i]);
        }

        /// <summary>
        /// Gets the 32-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public int GetInt32(int i)
        {
            return int.Parse(_enumerator.Current[i]);
        }

        /// <summary>
        /// Gets the 64-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public long GetInt64(int i)
        {
            return long.Parse(_enumerator.Current[i]);
        }

        /// <summary>
        /// Gets the name for the field to find.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public string GetName(int i)
        {
            return _responseInfo.Columns[i].Name;
        }

        /// <summary>
        /// Return the index of the named field.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public int GetOrdinal(string name)
        {
            return _responseInfo.Columns.FindIndex(ci => (ci.Name == name));
        }

        /// <summary>
        /// Gets the string value of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public string GetString(int i)
        {
            return _enumerator.Current[i];
        }

        /// <summary>
        /// Return the value of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public object GetValue(int i)
        {
            if (IsDBNull(i))
                return DBNull.Value;
            switch (_responseInfo.Columns[i].DataType)
            {
                case "smallint":
                    return GetInt16(i);
                case "int":
                    return GetInt32(i);
                case "bigint":
                    return GetInt64(i);
                case "double":
                    return GetDouble(i);
                case "real":
                    return GetFloat(0);
                case "timestamp":
                case "date":
                case "time":
                    return GetDateTime(i);
                default:
                    return GetString(i);
            }
        }

        /// <summary>
        /// Gets all the attribute fields in the collection for the current record.
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public int GetValues(object[] values)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        /// <summary>
        /// Return whether the specified field is set to null.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public bool IsDBNull(int i)
        {
            return _enumerator.Current[i] == "NULL";
        }

        /// <summary>
        /// Gets the specified column by column name.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public object this[string name]
        {
            get { return GetValue(GetOrdinal(name)); }
        }

        /// <summary>
        /// Gets the specified column by column index
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public object this[int i]
        {
            get { return GetValue(i); }
        }

        #endregion
    }
}
