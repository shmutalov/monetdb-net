using System.Collections.Generic;
using System.Data.MonetDb.Helpers.Mapi.Enums;
using System.Data.MonetDb.Helpers.Mapi.IO;
using System.Data.MonetDb.Helpers.Mapi.Protocols;
using System.IO;
using System.Net.Sockets;

namespace System.Data.MonetDb.Helpers.Mapi
{
    /// <summary>
    /// MapiSocket is a class for talking to a MonetDB server with the MAPI protocol.
    /// MAPI is a line oriented protocol that talks UTF8 so we wrap a TCP socket with
    /// StreamReader and StreamWriter streams to handle conversion.
    /// 
    /// MapiSocket logs into the MonetDB server, since the socket is worthless if it's
    /// not logged in.
    /// </summary>
    internal sealed class MapiSocket : IDisposable
    {
        public readonly DateTime Created;
        private TcpClient _socket;

        public MapiSocket()
        {
            Created = DateTime.Now;

            // register protocols
            MapiProtocolFactory.Register<MapiProtocolVersion8>(8);
            MapiProtocolFactory.Register<MapiProtocolVersion9>(9);
        }

        public void Close()
        {
            Dispose();
        }

        /// <summary>
        /// Connects to a given host.  Returns a list of any warnings from the server.
        /// </summary>
        /// <param name="host"></param>
        /// <param name="port"></param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public IList<string> Connect(string host, int port, string username, string password, string database)
        {
            Database = database;
            Host = host;
            Port = port;
            Username = username;

            _socket = new TcpClient(Host, Port)
            {
                NoDelay = true,
                ReceiveTimeout = 60 * 2 * 1000,
                SendBufferSize = 60 * 2 * 1000
            };

            FromDatabase = new StreamReader(new MonetDbStream(_socket.GetStream()));
            ToDatabase = new StreamWriter(new MonetDbStream(_socket.GetStream()));

            var challenge = FromDatabase.ReadLine();

            // wait till the prompt
            FromDatabase.ReadLine();

            var response = GetChallengeResponse(challenge, username, password, "sql", database, null);

            ToDatabase.WriteLine(response);
            ToDatabase.Flush();

            var temp = FromDatabase.ReadLine();
            var redirects = new List<string>();
            var warnings = new List<string>();

            while (temp != ".")
            {
                if (string.IsNullOrEmpty(temp))
                    throw new MonetDbException("Connection to the server was lost");

                switch ((MonetDbLineType)temp[0])
                {
                    case MonetDbLineType.Error:
                        throw new MonetDbException(temp.Substring(1));
                    case MonetDbLineType.Info:
                        warnings.Add(temp.Substring(1));
                        break;
                    case MonetDbLineType.Redirect:
                        warnings.Add(temp.Substring(1));
                        break;
                }

                temp = FromDatabase.ReadLine();
            }

            if (redirects.Count <= 0)
                return warnings;

            _socket.Client.Close();
            _socket.Close();

            return FollowRedirects(redirects, username, password);
        }


        public string Database { get; set; }

        private StreamReader FromDatabase { get; set; }

        public string Host { get; private set; }

        public int Port { get; private set; }

        private StreamWriter ToDatabase { get; set; }

        public string Username { get; private set; }

        public void Dispose()
        {
            if (ToDatabase != null && _socket.Connected)
                ToDatabase.Close();

            if (FromDatabase != null && _socket.Connected)
                FromDatabase.Close();

            _socket.Close();
        }

        internal IEnumerable<MonetDbQueryResponseInfo> ExecuteSql(string sql)
        {
            ToDatabase.Write("s" + sql + ";\n");
            ToDatabase.Flush();
            var re = new MonetDbResultEnumerator(FromDatabase);
            return re.GetResults();
        }

        /// <summary>
        /// Returns a response string that we should send to the MonetDB server upon initial connection.
        /// The challenge string is sent from the server in the format (without quotes) "challenge:servertype:protocolversion:"
        /// 
        /// For now we only support protocol version 8.
        /// </summary>
        /// <param name="challengeString">initial string sent from server to challenge against</param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <param name="language"></param>
        /// <param name="database"></param>
        /// <param name="hash">the hash method to use, or null for all supported hashes</param>
        /// <returns></returns>
        private string GetChallengeResponse(
            string challengeString, 
            string username, string password, 
            string language, string database, 
            string hash)
        {
            var tokens = challengeString.Split(':');

            if (tokens.Length <= 4)
            {
                throw new MonetDbException(string.Format(
                    "Server challenge unusable! Challenge contains too few tokens: {0}",
                    challengeString));
            }

            int version;

            if (!int.TryParse(tokens[2], out version))
                throw new MonetDbException("Unknown Mapi protocol {0}", tokens[2]);

            // get Mapi protocol instance
            var protocol = MapiProtocolFactory.GetProtocol(version);

            if (protocol == null)
                throw new MonetDbException("Unsupported protocol version {0}", version);

            return protocol.BuildChallengeResponse(username, password, 
                language, tokens, 
                database, hash);
        }

        /// <summary>
        /// We try the first url to redirect to.  It's not great, but realistically
        /// we shouldn't get too many redirect urls to redirect to.  Returns all the
        /// new warnings from the new connection.
        /// </summary>
        /// <param name="redirectUrls"></param>
        /// <param name="user"></param>
        /// <param name="password"></param>
        private IList<string> FollowRedirects(IReadOnlyList<string> redirectUrls, string user, string password)
        {
            var uri = new Uri(redirectUrls[0]);
            var host = uri.Host;
            var port = uri.Port;
            var database = uri.PathAndQuery.Replace(uri.Query, "");
            return Connect(host, port, user, password, database);
        } 
    }
}
