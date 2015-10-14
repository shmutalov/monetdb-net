/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is MonetDB .NET Client Library.
 * 
 * The Initial Developer of the Original Code is Tim Gebhardt <tim@gebhardtcomputing.com>.
 * Portions created by Tim Gebhardt are Copyright (C) 2007. All Rights Reserved.
 */

using System.Collections.Generic;
using System.Data.MonetDb.Helpers;
using System.Data.MonetDb.Helpers.Mapi;

namespace System.Data.MonetDb
{
    /// <summary>
    /// Represents an open connection with an MonetDB server.
    /// </summary>
    public class MonetDbConnection : IDbConnection
    {
        private string _host;
        private int _port;
        private string _username;
        private string _password;
        private int _minPoolConnections = 3;
        private int _maxPoolConnections = 20;

        private MapiSocket _socket;

        private MonetDbMetaData _metaData;
        private readonly object _syncLock = new object();

        #region Constructors

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
            State = ConnectionState.Closed;
        }

        #endregion

        /// <summary>
        /// Begins a database transaction with the specified <c>IsolationLevel</c> value.
        /// </summary>
        /// <param name="isolationLevel">One of the <c>IsolationLevel</c> values.</param>
        /// <returns></returns>
        public IDbTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            if (State != ConnectionState.Open)
                throw new InvalidOperationException("Connection is not open");

            if (isolationLevel != IsolationLevel.Serializable)
                throw new ArgumentException(string.Format(
                        "Isolation level {0} is not supported", 
                        isolationLevel), 
                    "isolationLevel");

            return new MonetDbTransaction(this, isolationLevel);
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
            var reopen = false;
            if (State == ConnectionState.Open)
            {
                Close();
                reopen = true;
            }

            var connectionStringChunks = ConnectionString.Split(';');
            for (var i = 0; i < connectionStringChunks.Length; i++)
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
            if (_socket != null)
                MonetDbConnectionFactory.CloseConnection(_socket, Database);

            State = ConnectionState.Closed;
        }

        private string _connectionString;
        /// <summary>
        /// Gets or sets the string used to open a database.
        /// </summary>
        /// <example>host=localhost;port=50000;username=admin;password=sa;database=demo;ssl=false;poolMinimum=3;poolMaximum=20</example>
        public string ConnectionString
        {
            get
            {
                return _connectionString;
            }
            set
            {
                if (string.IsNullOrEmpty(value))
                    throw new ArgumentNullException( "value", "ConnectionString cannot be null");

                _connectionString = value;
                ParseConnectionString(value);
            }
        }

        /// <summary>
        /// Gets the time to wait while trying to establish a 
        /// connection before terminating the attempt and generating an error.
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
            return new MonetDbCommand("", this);
        }

        /// <summary>
        /// Gets the name of the current database or the 
        /// database to be used after a connection is opened.
        /// </summary>
        public string Database { get; private set; }

        /// <summary>
        /// Opens a database connection with the settings 
        /// specified by the <c>ConnectionString</c> property 
        /// of the provider-specific Connection object.
        /// </summary>
        public void Open()
        {
            if (State == ConnectionState.Open)
                throw new InvalidOperationException("Connection is already open");

            State = ConnectionState.Connecting;

            if (string.IsNullOrEmpty(ConnectionString))
            {
                State = ConnectionState.Closed;
                throw new InvalidOperationException("ConnectionString has not been set. Cannot connect to database.");
            }

            _socket = MonetDbConnectionFactory.GetConnection(_host, _port, _username, _password,
                                                             Database, _minPoolConnections, _maxPoolConnections);

            State = ConnectionState.Open;
        }

        /// <summary>
        /// Gets the current state of the connection.
        /// </summary>
        public ConnectionState State { get; private set; }

        /// <summary>
        /// Releases the connection back to the connection pool.
        /// </summary>
        public void Dispose()
        {
            Close();
        }

        ///// <summary>
        ///// Returns the number of rows affected in an SQL UPDATE/DELETE/INSERT query
        ///// </summary>
        ///// <returns></returns>
        //internal int GetRowsAffected()
        //{
        //    throw new NotImplementedException("this is not implemented yet");
        //    //return MapiLib.MapiRowsAffected(_socket);
        //}

        internal MonetDbMetaData GetMetaData()
        {
            // create on request
            return _metaData ?? (_metaData = new MonetDbMetaData(this));

            // TODO: Finish the schema extraction
            //var dt = new DataTable();
            //dt.Columns.Add("ColumnName", typeof(string));
            //dt.Columns.Add("ColumnOrdinal", typeof(int));
            //dt.Columns.Add("ColumnSize", typeof(int));
            //dt.Columns.Add("NumericPrecision");
            //dt.Columns.Add("NumericScale");
            //dt.Columns.Add("IsUnique", typeof(bool));
            //dt.Columns.Add("IsKey", typeof(bool));
            //dt.Columns.Add("BaseServerName", typeof(string));
            //dt.Columns.Add("BaseCatalogName", typeof(string));
            //dt.Columns.Add("BaseColumnName", typeof(string));
            //dt.Columns.Add("BaseSchemaName", typeof(string));
            //dt.Columns.Add("BaseTableName", typeof(string));
            //dt.Columns.Add("BaseColumnName", typeof(string));
            //dt.Columns.Add("DataType", typeof(Type));
            //dt.Columns.Add("DataTypeName", typeof(string));

            //return dt;
        }

        private void ParseConnectionString(string connectionString)
        {
            _host = _username = _password = Database = null;
            _port = 50000;

            foreach (var setting in connectionString.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries))
            {
                var keyValue = setting.Split(new[] { '=' }, StringSplitOptions.RemoveEmptyEntries);
                if (keyValue.Length != 2)
                {
                    throw new ArgumentException(
                        string.Format("ConnectionString is not well-formed: {0}", setting), 
                        "connectionString");
                }

                var key = keyValue[0].ToLowerInvariant().Trim();
                var value = keyValue[1];

                switch (key)
                {
                    case "host":
                        _host = value;
                        break;
                    case "port":
                        if (!int.TryParse(value, out _port))
                        {
                            throw new ArgumentException(
                                string.Format("Port is not a valid integer: {0}", value),
                                "connectionString");
                        }
                        break;
                    case "username":
                        _username = value;
                        break;
                    case "password":
                        _password = value;
                        break;
                    case "database":
                        Database = value;
                        break;
                    case "poolminimum":
                        int tempPoolMin;
                        if (!int.TryParse(value, out tempPoolMin))
                        {
                            throw new ArgumentException(
                                string.Format("poolminimum is not a valid integer: {0}", value),
                                "connectionString");
                        }

                        if (tempPoolMin > _minPoolConnections)
                        {
                            _minPoolConnections = tempPoolMin;
                        }
                        break;
                    case "poolmaximum":
                        int tempPoolMax;
                        if (!int.TryParse(value, out tempPoolMax))
                        {
                            throw new ArgumentException(
                                string.Format("poolmaximum is not a valid integer: {0}", value),
                                "connectionString");
                        }

                        if (tempPoolMax > _maxPoolConnections)
                        {
                            _maxPoolConnections = tempPoolMax;
                        }
                        break;
                }
            }

            if (string.IsNullOrEmpty(Database))
            {
                throw new ArgumentException("Database name not specified. Please specify database.",
                    "connectionString");
            }
        }

        internal IEnumerable<MonetDbQueryResponseInfo> ExecuteSql(string sql)
        {
            return _socket.ExecuteSql(sql);
        }
    }
}
