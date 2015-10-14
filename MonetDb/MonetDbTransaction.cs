namespace System.Data.MonetDb
{
    /// <summary>
    /// Represents a local transaction.
    /// </summary>
    public class MonetDbTransaction : IDbTransaction
    {
        private readonly MonetDbConnection _connection;
        private readonly IsolationLevel _isolation;

        private readonly object _syncLock = new object();

        #region IDbTransaction Members

        /// <summary>
        /// Initializes a new transaction with the MonetDB server with this particular connection.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="isolationLevel"></param>
        internal MonetDbTransaction(MonetDbConnection connection, IsolationLevel isolationLevel)
        {
            // MonetDb only support "Serializable" isolation level
            if (isolationLevel != IsolationLevel.Serializable)
                throw new ArgumentException(string.Format(
                        "Isolation level {0} is not supported", 
                        isolationLevel), 
                    "isolationLevel");

            if (connection == null)
                throw new ArgumentNullException("connection");

            _connection = connection;
            _isolation = IsolationLevel;

            Start();
        }

        /// <summary>
        /// Checks connection
        /// </summary>
        private void CheckConnection()
        {
            if (_connection == null || 
                _connection.State != ConnectionState.Open)
                throw new MonetDbException("Connection unexpectedly disposed or closed");
        }

        /// <summary>
        /// Start the database transaction
        /// </summary>
        private void Start()
        {
            lock (_syncLock)
            {
                CheckConnection();

                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = "START TRANSACTION;";
                    command.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Commits the database transaction.
        /// </summary>
        public void Commit()
        {
            lock (_syncLock)
            {
                CheckConnection();

                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = "COMMIT;";
                    command.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Specifies the Connection object to associate with the transaction
        /// </summary>
        public IDbConnection Connection
        {
            get { return _connection; }
        }

        /// <summary>
        /// Specifies the <c>IsolationLevel</c> for this transaction
        /// </summary>
        public IsolationLevel IsolationLevel
        {
            get { return _isolation; }
        }

        /// <summary>
        /// Rolls back a transaction from a pending state.
        /// </summary>
        public void Rollback()
        {
            lock (_syncLock)
            {
                CheckConnection();

                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = "ROLLBACK;";
                    command.ExecuteNonQuery();
                }
            }
        }

        #endregion

        #region IDisposable Members

        /// <summary>
        /// Rolls back the transaction (if uncommited) 
        /// and releases the resources that were used for the transaction.
        /// </summary>
        public void Dispose()
        {
            
        }

        #endregion
    }
}
