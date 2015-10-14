using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace System.Data.MonetDb.Helpers
{
    internal class MonetDbMetaData : IDisposable
    {
        private readonly MonetDbConnection _connection;
        private readonly object _syncLock = new object();

        public MonetDbMetaData(MonetDbConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            _connection = connection;
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

        public void Dispose()
        {
            
        }
    }
}
