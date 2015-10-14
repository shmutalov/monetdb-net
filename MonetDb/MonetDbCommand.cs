using System.Collections.Generic;
using System.Data.MonetDb.Helpers.Mapi;
using System.Text;

namespace System.Data.MonetDb
{
    /// <summary>
    /// Represents an SQL command to send to a <c>MonetDbConnection</c>
    /// </summary>
    public class MonetDbCommand : IDbCommand
    {
        /// <summary>
        /// Initializes a new command
        /// </summary>
        public MonetDbCommand()
        {
            _parameters = new MonetDbParameterCollection();
        }

        /// <summary>
        /// Initializes a new command
        /// </summary>
        /// <param name="cmdText"></param>
        public MonetDbCommand(string cmdText)
            : this()
        {
            CommandText = cmdText;
        }

        /// <summary>
        /// Initializes a new command.
        /// </summary>
        /// <param name="cmdText"></param>
        /// <param name="connection"></param>
        public MonetDbCommand(string cmdText, MonetDbConnection connection)
            : this(cmdText)
        {
            Connection = connection;
        }

        /// <summary>
        /// Initializes a new command.
        /// </summary>
        /// <param name="cmdText"></param>
        /// <param name="connection"></param>
        /// <param name="transaction"></param>
        public MonetDbCommand(string cmdText, MonetDbConnection connection, MonetDbTransaction transaction)
            : this(cmdText, connection)
        {
            Transaction = transaction;
        }

        #region IDbCommand Members

        /// <summary>
        /// Attempts to cancels the execution of this <c>MonetDbCommand</c>.
        /// </summary>
        public void Cancel()
        {
            throw new NotImplementedException("this is not implemented yet");
        }

        /// <summary>
        /// Gets or sets the text command to run against the MonetDB server.
        /// </summary>
        public string CommandText { get; set; }

        /// <summary>
        /// Gets or sets the wait time before terminating the attempt to execute a command and generating an error.
        /// </summary>
        public int CommandTimeout
        {
            get { throw new NotSupportedException("Getting a timeout is not supported yet."); }
            set { throw new NotSupportedException("Setting a timeout is not supported yet."); }
        }

        /// <summary>
        /// Indicates or specifies how the <c>CommandText</c> property is interpreted.
        /// </summary>
        public CommandType CommandType
        {
            get
            {
                throw new Exception("The method or operation is not implemented.");
            }
            set
            {
                throw new Exception("The method or operation is not implemented.");
            }
        }

        private MonetDbConnection _connection;
        /// <summary>
        /// Gets or sets the <c>IDbConnection</c> used by this instance of the <c>IDbCommand</c>.
        /// </summary>
        public IDbConnection Connection
        {
            get { return _connection; }
            set { _connection = (MonetDbConnection)value; }
        }

        /// <summary>
        /// Creates a new instance of an <c>IDbDataParameter</c> object.
        /// </summary>
        /// <returns></returns>
        public IDbDataParameter CreateParameter()
        {
            return new MonetDbParameter();
        }

        /// <summary>
        /// Executes an SQL statement against the <c>Connection</c> object of MonetDB data provider, 
        /// and returns the number of rows affected.
        /// </summary>
        /// <returns></returns>
        public int ExecuteNonQuery()
        {
            var cnt = 0;
            using (var dr = new MonetDbDataReader(ExecuteCommand(), Connection as MonetDbConnection))
            {
                do
                {
                    cnt += dr.RecordsAffected;
                } while (dr.NextResult());
            }
            return cnt;
        }

        private IEnumerable<MonetDbQueryResponseInfo> ExecuteCommand()
        {
            if (Connection == null || Connection.State != ConnectionState.Open)
                throw new MonetDbException("Connection is closed");

            var sb = new StringBuilder(CommandText);
            foreach (MonetDbParameter p in Parameters)
                ApplyParameter(sb, new KeyValuePair<string, string>(p.ParameterName, p.GetProperParameter()));

            return (Connection as MonetDbConnection).ExecuteSql(sb.ToString());
        }

        /// <summary>
        /// Executes the <c>CommandText</c> against the <c>Connection</c> and builds an <c>IDataReader</c>.
        /// </summary>
        /// <param name="behavior"></param>
        /// <returns></returns>
        public IDataReader ExecuteReader(CommandBehavior behavior)
        {
            return new MonetDbDataReader(ExecuteCommand(), Connection as MonetDbConnection);
        }

        /// <summary>
        /// Executes the <c>CommandText</c> against the <c>Connection</c> and builds an <c>IDataReader</c>.
        /// </summary>
        /// <returns></returns>
        public IDataReader ExecuteReader()
        {
            return ExecuteReader(CommandBehavior.Default);
        }

        private StringBuilder ApplyParameter(StringBuilder sb, KeyValuePair<string, string> p)
        {
            var quoteSingle = 0;
            var quoteDouble = 0;
            var param = p.Key.ToCharArray();
            var i = 0;
            while (i < sb.Length)
            {
                var cur = sb[i];
                if (cur == '\'')
                    quoteSingle++;
                if (cur == '\"')
                    quoteDouble++;
                if (quoteDouble + quoteSingle == 0)
                {
                    var found = true;
                    for (var j = 0; j < param.Length; j++)
                        if (param[j] != sb[i + j])
                        {
                            found = false;
                            break;
                        }

                    if (found)
                    {
                        sb.Remove(i, param.Length);
                        sb.Insert(i, p.Value);
                        i--;
                    }
                }
                i++;
            }

            return sb;
        }


        /// <summary>
        /// Executes the query, and returns the first column of the first row in the resultset 
        /// returned by the query. Extra columns or rows are ignored.
        /// </summary>
        /// <returns></returns>
        public object ExecuteScalar()
        {
            using (var dr = ExecuteReader())
            {
                return dr.Read() ? dr[0] : null;
            }
        }

        private readonly MonetDbParameterCollection _parameters;
        /// <summary>
        /// Gets the IDataParameterCollection.
        /// </summary>
        public IDataParameterCollection Parameters
        {
            get { return _parameters; }
        }

        /// <summary>
        /// Creates a prepared (or compiled) version of the command on the data source.
        /// </summary>
        public void Prepare()
        {

        }

        private IDbTransaction _transaction;
        /// <summary>
        /// Gets or sets the transaction within which this Command object executes.
        /// </summary>
        public IDbTransaction Transaction
        {
            get { return _transaction; }
            set { _transaction = (MonetDbTransaction)value; }
        }

        /// <summary>
        /// Gets or sets how command results are applied to the <c>DataRow</c> when used by the 
        /// <c>Update</c> method of a <c>MonetDbDataAdapter</c>.
        /// </summary>
        public UpdateRowSource UpdatedRowSource
        {
            get
            {
                throw new Exception("The method or operation is not implemented.");
            }
            set
            {
                throw new Exception("The method or operation is not implemented.");
            }
        }

        #endregion

        #region IDisposable Members

        /// <summary>
        /// Releases the resources used by this command.
        /// </summary>
        public void Dispose()
        {

        }

        #endregion
    }
}
