using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace MonetDb
{
    /// <summary>
    /// Represents a local transaction.
    /// </summary>
    public class MonetDbTransaction : IDbTransaction
    {
        #region IDbTransaction Members

        /// <summary>
        /// Initializes a new transaction with the MonetDB server with this particular connection.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="isolationLevel"></param>
        internal MonetDbTransaction(MonetDbConnection connection, IsolationLevel isolationLevel)
        {

        }

        /// <summary>
        /// Commits the database transaction.
        /// </summary>
        public void Commit()
        {
            throw new Exception("The method or operation is not implemented.");
        }

        /// <summary>
        /// Specifies the Connection object to associate with the transaction
        /// </summary>
        public IDbConnection Connection
        {
            get { throw new Exception("The method or operation is not implemented."); }
        }

        /// <summary>
        /// Specifies the <c>IsolationLevel</c> for this transaction
        /// </summary>
        public IsolationLevel IsolationLevel
        {
            get { throw new Exception("The method or operation is not implemented."); }
        }

        /// <summary>
        /// Rolls back a transaction from a pending state.
        /// </summary>
        public void Rollback()
        {
            throw new Exception("The method or operation is not implemented.");
        }

        #endregion

        #region IDisposable Members

        /// <summary>
        /// Rolls back the transaction (if uncommited) and releases the resources that were used for the transaction.
        /// </summary>
        public void Dispose()
        {
            throw new Exception("The method or operation is not implemented.");
        }

        #endregion
    }
}
