using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace MonetDb
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

        private string _commandText;
        /// <summary>
        /// Gets or sets the text command to run against the MonetDB server.
        /// </summary>
        public string CommandText
        {
            get { return _commandText; }
            set { _commandText = value; }
        }

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
            throw new NotImplementedException("this is not implemented yet");
        }

        /// <summary>
        /// Executes the <c>CommandText</c> against the <c>Connection</c> and builds an <c>IDataReader</c>.
        /// </summary>
        /// <param name="behavior"></param>
        /// <returns></returns>
        public IDataReader ExecuteReader(CommandBehavior behavior)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        /// <summary>
        /// Executes the <c>CommandText</c> against the <c>Connection</c> and builds an <c>IDataReader</c>.
        /// </summary>
        /// <returns></returns>
        public IDataReader ExecuteReader()
        {
            return ExecuteReader(CommandBehavior.Default);
        }

        /// <summary>
        /// Executes the query, and returns the first column of the first row in the resultset 
        /// returned by the query. Extra columns or rows are ignored.
        /// </summary>
        /// <returns></returns>
        public object ExecuteScalar()
        {
            throw new Exception("The method or operation is not implemented.");
        }

        private MonetDbParameterCollection _parameters;
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
            throw new NotImplementedException("this is not implemented yet");
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
            throw new NotImplementedException("this is not implemented yet");
        }

        #endregion
    }
}
