using System;
using System.Collections.Generic;
using System.Text;

namespace MonetDb
{
    /// <summary>
    /// Handles the accounting for the connections to the database.  Handles the connection
    /// pooling of the connections.
    /// </summary>
    internal class MonetDbConnectionFactory
    {
        private MonetDbConnectionFactory()
        {

        }

        /// <summary>
        /// Returns a connection from the connection pool.
        /// </summary>
        /// <param name="host"></param>
        /// <param name="port"></param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <param name="database"></param>
        /// <param name="useSsl"></param>
        /// <returns></returns>
        public static MapiConnection GetConnection(string host, int port, string username, string password, string database, bool useSsl)
        {
            MapiConnection retval;
            if (useSsl)
                retval = MapiLib.MapiConnect(host, port, username, password, "sql", database);
            else
                retval = MapiLib.MapiConnectSsl(host, port, username, password, "sql", database);

            if (retval.Ptr == IntPtr.Zero)
            {
                string error = MapiLib.MapiErrorString(retval);
                throw new MonetDbException(error);
            }

            return retval;
        }

        public static void CloseConnection(MapiConnection connection)
        {
            MapiLib.MapiDisconnect(connection);
            MapiLib.MapiDestroy(connection);
        }
    }
}
