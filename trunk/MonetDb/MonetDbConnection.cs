using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace MonetDb
{
    public class MonetDbConnection : IDbConnection
    {
        public MonetDbConnection()
        { }

        public MonetDbConnection(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString", "connectionString cannot be null");

            ConnectionString = connectionString;
        }

        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        public IDbTransaction BeginTransaction()
        {
            throw new Exception("The method or operation is not implemented.");
        }

        public void ChangeDatabase(string databaseName)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        public void Close()
        {
            if (_connection.Ptr != IntPtr.Zero)
            {
                MonetDbConnectionFactory.CloseConnection(_connection);
                _connection.Ptr = IntPtr.Zero;
            }
        }

        private string _connectionString;
        /// <summary>
        /// The connection string used to connect to the MonetDB server.  
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

        public int ConnectionTimeout
        {
            get { throw new Exception("The method or operation is not implemented."); }
        }

        public IDbCommand CreateCommand()
        {
            throw new Exception("The method or operation is not implemented.");
        }

        public string Database
        {
            get { throw new Exception("The method or operation is not implemented."); }
        }

        public void Open()
        {
            if (_connection.Ptr == IntPtr.Zero)
                _connection = MonetDbConnectionFactory.GetConnection(_host, _port, _username, _password, _dbname, _useSsl);
            else
                throw new InvalidOperationException("Connection is already open");
        }

        public ConnectionState State
        {
            get { throw new Exception("The method or operation is not implemented."); }
        }

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
    }
}
