using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace MonetDb
{
    /// <summary>
    /// Represents an open connection with an MonetDB server.
    /// </summary>
    public class MonetDbConnection : IDbConnection
    {
        /// <summary>
        /// Initializes a new connection with the MonetDB server.
        /// </summary>
        public MonetDbConnection()
        { }

        /// <summary>
        /// Initializes a new connection with the MonetDB server.
        /// </summary>
        /// <param name="connectionString">
        /// The information used to establish a connection.  
        /// See <c>ConnectionString</c> for the valid formatting of this parameter.
        /// </param>
        public MonetDbConnection(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString", "connectionString cannot be null");

            ConnectionString = connectionString;
            _connectionState = ConnectionState.Closed;
        }
        /// <summary>
        /// Begins a database transaction with the specified <c>IsolationLevel</c> value.
        /// </summary>
        /// <param name="il">One of the <c>IsolationLevel</c> values.</param>
        /// <returns></returns>
        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            if (_connection.Ptr == IntPtr.Zero || _connectionState != ConnectionState.Open)
                throw new InvalidOperationException("Connection is not open");

            return null;
        }

        /// <summary>
        /// Begins a database transaction.
        /// </summary>
        /// <returns></returns>
        public IDbTransaction BeginTransaction()
        {
            return BeginTransaction(IsolationLevel.ReadUncommitted);
        }

        /// <summary>
        /// Changes the current database for an open MonetDbConnection object.
        /// </summary>
        /// <param name="databaseName">
        /// The name of the database to use in place of the current database.
        /// </param>
        public void ChangeDatabase(string databaseName)
        {
            bool reopen = false;
            if (_connection.Ptr != IntPtr.Zero && _connectionState == ConnectionState.Open)
            {
                Close();
                reopen = true;
            }

            string[] connectionStringChunks = ConnectionString.Split(';');
            for (int i = 0; i < connectionStringChunks.Length; i++)
                if (connectionStringChunks[i].StartsWith("database=", StringComparison.InvariantCultureIgnoreCase))
                    connectionStringChunks[i] = "database=" + databaseName;

            ConnectionString = string.Join(";", connectionStringChunks);

            if (reopen)
                Open();
        }

        /// <summary>
        /// Releases the connection back to the connection pool.
        /// </summary>
        public void Close()
        {
            if (_connection.Ptr != IntPtr.Zero)
            {
                MonetDbConnectionFactory.CloseConnection(_connection);
                _connection.Ptr = IntPtr.Zero;
            }

            _connectionState = ConnectionState.Closed;
        }

        private string _connectionString;
        /// <summary>
        /// Gets or sets the string used to open a database.
        /// </summary>
        /// <example>host=localhost;port=50000;username=admin;password=sa;database=demo;ssl=false</example>
        public string ConnectionString
        {
            get
            {
                return _connectionString;
            }
            set
            {
                if (string.IsNullOrEmpty(value))
                    throw new ArgumentNullException("ConnectionString", "ConnectionString cannot be null");

                _connectionString = value;
                ParseConnectionString(value);
            }
        }

        /// <summary>
        /// Gets the time to wait while trying to establish a connection before terminating the attempt and generating an error.
        /// </summary>
        public int ConnectionTimeout
        {
            get { return 60; }
        }

        /// <summary>
        /// Creates and returns a Command object associated with the connection.
        /// </summary>
        /// <returns></returns>
        public IDbCommand CreateCommand()
        {
            throw new Exception("The method or operation is not implemented.");
        }

        /// <summary>
        /// Gets the name of the current database or the database to be used after a connection is opened.
        /// </summary>
        public string Database
        {
            get { return _dbname; }
        }

        /// <summary>
        /// Opens a database connection with the settings specified by the <c>ConnectionString</c> property 
        /// of the provider-specific Connection object.
        /// </summary>
        public void Open()
        {
            _connectionState = ConnectionState.Connecting;

            if (string.IsNullOrEmpty(ConnectionString))
            {
                _connectionState = ConnectionState.Closed;
                throw new InvalidOperationException("ConnectionString has not been set.  Cannot connect to database.");
            }

            if (_connection.Ptr == IntPtr.Zero)
                _connection = MonetDbConnectionFactory.GetConnection(_host, _port, _username, _password, _dbname, _useSsl);
            else
                throw new InvalidOperationException("Connection is already open");

            _connectionState = ConnectionState.Open;
        }

        /// <summary>
        /// Gets the current state of the connection.
        /// </summary>
        public ConnectionState State
        {
            get { return _connectionState; }
        }

        /// <summary>
        /// Releases the connection back to the connection pool.
        /// </summary>
        public void Dispose()
        {
            Close();
        }

        private void ParseConnectionString(string connectionString)
        {
            _host = _username = _password = _dbname = null;
            _port = 0;

            foreach (string setting in connectionString.Split(';'))
            {
                string[] key_value = setting.Split('=');
                if (key_value.Length != 2)
                {
                    throw new ArgumentException(
                        string.Format("ConnectionString is not well-formed: {0}", setting), "ConnectionString");
                }

                string key = key_value[0].ToLowerInvariant();
                string value = key_value[1];

                switch (key)
                {
                    case "host": _host = value;
                        break;
                    case "port": if (!int.TryParse(value, out _port))
                        {
                            throw new ArgumentException(
                                string.Format("Port is not a valid integer: {0}", value),
                                "ConnectionString");
                        }
                        break;
                    case "username": _username = value;
                        break;
                    case "password": _password = value;
                        break;
                    case "database": _dbname = value;
                        break;
                    case "ssl": if (!bool.TryParse(value, out _useSsl))
                        {
                            throw new ArgumentException(
                                string.Format("ssl is not a valid boolean: {0}", value),
                                "ConnectionString");
                        }
                        break;
                    default:
                        break;
                }
            }

            if (string.IsNullOrEmpty(_dbname))
            {
                throw new ArgumentException("Database name not specified.  Please specify database.", "ConnectionString");
            }
        }

        private string _host;
        private int _port;
        private string _username;
        private string _password;
        private string _dbname;
        private bool _useSsl;

        private MapiConnection _connection;

        private ConnectionState _connectionState;
    }
}
