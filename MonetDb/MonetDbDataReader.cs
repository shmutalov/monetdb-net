using System.Collections.Generic;
using System.Data.MonetDb.Helpers.Mapi;

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

        internal MonetDbDataReader(IEnumerable<MonetDbQueryResponseInfo> ri, MonetDbConnection connection)
        {
            _connection = connection;
            _responseInfoEnumerable = ri;
            _responeInfoEnumerator = ri.GetEnumerator();
            NextResult();
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
            var schema = new DataTable("SchemaTable");

            return schema;

            //return _connection.GetObjectSchema(_responseInfo.Columns);

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
                while (_enumerator.MoveNext()) ;
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
