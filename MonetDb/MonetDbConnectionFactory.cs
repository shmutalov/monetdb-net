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
using System.IO;
using System.Timers;

namespace System.Data.MonetDb
{
    /// <summary>
    /// Handles the accounting for the connections to the database.  Handles the connection
    /// pooling of the connections.
    /// </summary>
    internal static class MonetDbConnectionFactory
    {
        private static readonly Dictionary<string, ConnectionInformation> ConnectionInfo = new Dictionary<string, ConnectionInformation>();

        private static readonly Timer MaintenanceTimer = new Timer(1000);

        private static void OnMaintenanceTimerElapsed(object sender, ElapsedEventArgs e)
        {
            MaintenanceTimer.Stop();
            ICollection<ConnectionInformation> connections;

            lock (ConnectionInfo)
            {
                connections = ConnectionInfo.Values;
            }

            foreach (var ci in connections)
            {
                lock (ci)
                {
                    for (var i = 0; i < ci.Active.Count + ci.Busy.Count - ci.Min && ci.Active.Count > 0; i++)
                    {
                        var socket = ci.Active.Peek();
                        if (socket.Created > DateTime.Now.AddMinutes(5))
                            socket.Close();
                    }
                }
            }
            MaintenanceTimer.Start();
        }

        private static void OnDomainUpload(object sender, EventArgs e)
        {
            ICollection<ConnectionInformation> connections;

            lock (ConnectionInfo)
            {
                connections = ConnectionInfo.Values;
            }

            foreach (var ci in connections)
            {
                lock (ci)
                {
                    for (var i = 0; i < ci.Active.Count + ci.Busy.Count; i++)
                    {
                        ci.Active.Dequeue(60000).Close();
                    }
                }
            }
        }

        static MonetDbConnectionFactory()
        {
            MaintenanceTimer.Elapsed += OnMaintenanceTimerElapsed;

            MaintenanceTimer.Start();

            AppDomain.CurrentDomain.DomainUnload += OnDomainUpload;
        }

        /// <summary>
        /// Returns a connection from the connection pool.
        /// </summary>
        /// <param name="host"></param>
        /// <param name="port"></param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <param name="database"></param>
        /// <param name="maxConn"></param>
        /// <param name="minConn"></param>
        /// <returns></returns>
        public static MapiSocket GetConnection(string host, int port, string username, string password,
                                                   string database, int minConn, int maxConn)
        {
            if (minConn < 1)
                throw new ArgumentOutOfRangeException("minConn",
                    minConn + "", "The minimum number of connections cannot be less than 1");
            if (maxConn < 1)
                throw new ArgumentOutOfRangeException("maxConn",
                    maxConn + "", "The mamimum number of connections cannot be less than 1");
            if (minConn > maxConn)
                throw new ArgumentException("The maximum number of connections cannot be greater than the minimum number of connections");

            SetupConnections(host, port, username, password, database, minConn, maxConn);

            ConnectionInformation info;
            var key = GetConnectionPoolKey(host, port, username, database);
            lock (ConnectionInfo)
            {
                info = ConnectionInfo[key];
            }

            MapiSocket retval;
            lock (info)
            {
                retval = info.Active.Dequeue();
                info.Busy.Add(retval);
            }
            return retval;
        }

        public static void CloseConnection(MapiSocket socket, string database)
        {
            var key = GetConnectionPoolKey(socket.Host, socket.Port, socket.Username, database);

            ConnectionInformation info;
            lock (ConnectionInfo)
            {
                info = ConnectionInfo[key];
            }

            lock (info)
            {
                info.Busy.Remove(socket);
                info.Active.Enqueue(socket);
            }
        }

        private static void SetupConnections(string host, int port, string username, string password,
                                             string database, int minConn, int maxConn)
        {
            var key = GetConnectionPoolKey(host, port, username, database);
            ConnectionInformation info;
            lock (ConnectionInfo)
            {
                if (!ConnectionInfo.TryGetValue(key, out info))
                    info = ConnectionInfo[key] = new ConnectionInformation(minConn, maxConn);
            }

            lock (info)
            {
                for (var i = info.Active.Count + info.Busy.Count;
                    i < info.Min || (info.Active.Count == 0 && i < info.Max);
                    i++)
                {
                    try
                    {
                        var socket = new MapiSocket();
                        socket.Connect(host, port, username, password, database);
                        info.Active.Enqueue(socket);
                    }
                    catch (IOException ex)
                    {
                        throw new MonetDbException(ex, "Problem connecting to the MonetDB server.");
                    }

                }
            }
        }

        private static string GetConnectionPoolKey(string host, int port, string username, string database)
        {
            return string.Format("{0}_{1}_{2}_{3}", host, port, username, database);
        }

        private class ConnectionInformation
        {
            public readonly BlockingQueue<MapiSocket> Active = new BlockingQueue<MapiSocket>();
            public readonly List<MapiSocket> Busy = new List<MapiSocket>();
            public readonly int Min;
            public readonly int Max;

            public ConnectionInformation(int min, int max)
            {
                Min = min;
                Max = max;
            }
        }
    }
}
