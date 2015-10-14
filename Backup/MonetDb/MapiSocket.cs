using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

namespace MonetDb
{

    class MonetDBQueryResponseInfo
    {
        public int id;
        public int columnCount;
        public int rowCount;
        public int tupleCount;
        public int recordsAffected;
        public List<MonetDBColumnInfo> columns;
        public IEnumerable<List<string>> data;
    }

    class MonetDBColumnInfo
    {
        public string name;
        public string dataType;
        public string tableName;
        public int length;
    }

    /// <summary>
    /// Represents the types of data sequences that can be returned from a MonetDB server.
    /// </summary>
    public enum MonetDbLineType
    {
        /// <summary>
        /// "there is currently no line", or the the type is unknown
        /// </summary>
        Unknown = (byte)0,
        /// <summary>
        /// A line starting with '!'
        /// </summary>
        Error = (byte)'!',
        /// <summary>
        /// A line starting with '%'
        /// </summary>
        Header = (byte)'%',
        /// <summary>
        /// A line starting with '['
        /// </summary>
        Result = (byte)'[',
        /// <summary>
        /// A line starting with '.'
        /// </summary>
        Prompt = (byte)'.',
        /// <summary>
        /// A line starting with ','
        /// </summary>
        More = (byte)',',
        /// <summary>
        /// A line starting with '&amp;', indicating the start of a header block
        /// </summary>
        SOHeader = (byte)'&',
        /// <summary>
        /// A line starting with '^'
        /// </summary>
        Redirect = (byte)'^',
        /// <summary>
        /// A line starting with '#'
        /// </summary>
        Info = (byte)'#'
    }

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
            _database = database;
            _host = host;
            _port = port;
            _username = username;

            _socket = new TcpClient(Host, Port);
            _socket.NoDelay = true;
            _socket.ReceiveTimeout = 60 * 2 * 1000;
            _socket.SendBufferSize = 60 * 2 * 1000;
            _fromDatabase = new StreamReader(new MonetDbStream(_socket.GetStream()));
            _toDatabase = new StreamWriter(new MonetDbStream(_socket.GetStream()));
            string challenge = _fromDatabase.ReadLine();
            string prompt = _fromDatabase.ReadLine();
            string response = GetChallengeResponse(challenge, username, password, "sql", database, null);
            _toDatabase.WriteLine(response);
            _toDatabase.Flush();
            string temp = _fromDatabase.ReadLine();
            List<string> redirects = new List<string>();
            List<string> warnings = new List<string>();
            while (temp != ".")
            {
                if (string.IsNullOrEmpty(temp))
                    throw new MonetDbException("Connection to the server was lost");
                switch ((byte)temp[0])
                {
                    case (byte)MonetDbLineType.Error:
                        throw new MonetDbException(temp.Substring(1));
                    case (byte)MonetDbLineType.Info:
                        warnings.Add(temp.Substring(1));
                        break;
                    case (byte)MonetDbLineType.Redirect:
                        warnings.Add(temp.Substring(1));
                        break;
                    default:
                        //not prepared for this
                        break;
                }
                temp = _fromDatabase.ReadLine();
            }

            if (redirects.Count > 0)
            {
                _socket.Client.Close();
                _socket.Close();
                return FollowRedirects(redirects, username, password);
            }
            else
                return warnings;
        }


        private string _database;
        public string Database
        {
            get { return _database; }
            set { _database = value; }
        }

        private StreamReader _fromDatabase;
        public StreamReader FromDatabase
        {
            get { return _fromDatabase; }
        }

        private string _host;
        public string Host
        {
            get { return _host; }
        }

        private int _port;
        public int Port
        {
            get { return _port; }
        }

        private StreamWriter _toDatabase;
        public StreamWriter ToDatabase
        {
            get { return _toDatabase; }
        }

        private string _username;
        public string Username
        {
            get { return _username; }
        }

        public void Dispose()
        {
            if (ToDatabase != null && _socket.Connected)
                ToDatabase.Close();
            if (FromDatabase != null && _socket.Connected)
                FromDatabase.Close();
            _socket.Close();
        }


        internal IEnumerable<MonetDBQueryResponseInfo> ExecuteSQL(string sql)
        {
            _toDatabase.Write("s" + sql + ";\n");
            _toDatabase.Flush();
            MonetDBResultEnumerator re = new MonetDBResultEnumerator(_fromDatabase);
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
        private string GetChallengeResponse(string challengeString, string username, string password, string language, string database, string hash)
        {
            int version = 0;
            string response;
            string[] tokens = challengeString.Split(':');
            if (tokens.Length <= 4)
                throw new MonetDbException("Server challenge unusable!  Challenge contains too few tokens: " + challengeString);
            string challenge = tokens[0];
            string serverType = tokens[1];
            if (!int.TryParse(tokens[2], out version))
                throw new MonetDbException("Invalid Protocol Version: " + tokens[2]);
            string hashes = hash == null ? tokens[3] : hash;

            if (version != 8)
                throw new MonetDbException("Unsupported protocol version");
            //This "merovingian is in the Java client, but I'm not quite sure what it's all about
            if (serverType.Equals("merovingian"))
            {
                username = "merovingian";
                password = "merovingian";
            }

            string pwhash = string.Format("{0}{1}{2}", "{plain}", password, challenge);
            if (hashes.Contains("SHA1"))
            {
                SHA1CryptoServiceProvider sha1 = new SHA1CryptoServiceProvider();
                byte[] hashBytes = sha1.ComputeHash(UTF8Encoding.UTF8.GetBytes(password + challenge));
                pwhash = string.Format("{0}{1}", "{SHA1}", ToHexString(hashBytes));
            }
            else if (hashes.Contains("MD5"))
            {
                MD5CryptoServiceProvider md5 = new MD5CryptoServiceProvider();
                byte[] hashBytes = md5.ComputeHash(UTF8Encoding.UTF8.GetBytes(password + challenge));
                pwhash = string.Format("{0}{1}", "{MD5}", ToHexString(hashBytes));
            }
            else if (hashes.Contains("plain"))
            {
                pwhash = string.Format("{0}{1}{2}", "{plain}", password, challenge);
            }
            else
            {
                throw new MonetDbException("No supported hashes in " + hashes);
            }

            if (tokens.Length > 4)
            {
                if (tokens[4].Equals("BIG"))
                {
                    //byte order of server is big-endian
                }
                else if (tokens[4].Equals("LIT"))
                {
                    //byte order of server is little-endian
                }
                else
                {
                    throw new MonetDbException("Invalid byte-order from server");
                }
            }

            response = string.Format("LIT:{0}:{1}:{2}:{3}:", username, pwhash, language, database);

            return response;
        }

        /// <summary>
        /// We try the first url to redirect to.  It's not great, but realistically
        /// we shouldn't get too many redirect urls to redirect to.  Returns all the
        /// new warnings from the new connection.
        /// </summary>
        /// <param name="redirectUrls"></param>
        /// <param name="user"></param>
        /// <param name="password"></param>
        private IList<string> FollowRedirects(List<string> redirectUrls, string user, string password)
        {
            Uri uri = new Uri(redirectUrls[0]);
            string host = uri.Host;
            int port = uri.Port;
            string database = uri.PathAndQuery.Replace(uri.Query, "");
            return Connect(host, port, user, password, database);
        }

        private string ToHexString(byte[] bytes)
        {
            string[] retval = new string[bytes.Length];
            for (int i = 0; i < bytes.Length; i++)
                retval[i] = bytes[i].ToString("X2");
            return string.Join("", retval);
        }

        /// <summary>
        /// This class process the stream into enumerated list of the MonetDBQueryResponseInfo objects which represent executed
        /// statements in the batch. IEnumerable is used to facilitate lazy execution and eliminate the need in materialization
        /// of the results returned by the server.
        /// </summary>
        private class MonetDBResultEnumerator
        {
            private string _temp;
            private StreamReader _stream;

            public MonetDBResultEnumerator(StreamReader stream)
            {
                _stream = stream;
            }

            private IEnumerable<List<string>> GetRows()
            {
                while (_temp[0] == '[')
                {
                    yield return SplitDataInColumns(_temp);
                    _temp = _stream.ReadLine();
                }
            }


            internal MonetDBQueryResponseInfo GetQueryResponseInfo(string s)
            {
                string[] s_parts = s.Substring(1).Split(new char[] { ' ' });
                switch (s_parts[0])
                {
                    case "1":
                    case "5":
                        {
                            MonetDBQueryResponseInfo qri = new MonetDBQueryResponseInfo();
                            qri.id = int.Parse(s_parts[1]);
                            qri.tupleCount = int.Parse(s_parts[2]);
                            qri.columnCount = int.Parse(s_parts[3]);
                            qri.rowCount = int.Parse(s_parts[4]);
                            return qri;
                        }
                }
                return new MonetDBQueryResponseInfo();
            }

            internal static IEnumerable<string> SplitCommaTabs(string s)
            {
                foreach (string v in s.Split(new char[] { ',' }))
                    yield return v.Trim(' ', '\t');
            }

            internal static IEnumerable<string> ExtractValuesList(string s, string start, string end)
            {
                int startIndex = s.IndexOf(start);
                int endIndex = s.IndexOf(end);
                return SplitCommaTabs(s.Substring(startIndex + 1, endIndex - startIndex - 1));
            }

            internal static KeyValuePair<string, List<string>> SplitColumnInfoLine(string s)
            {
                return new KeyValuePair<string, List<string>>(s.Substring(s.IndexOf('#') + 1).Trim(), new List<string>(ExtractValuesList(s, "%", "#")));
            }

            internal static List<string> SplitDataInColumns(string s)
            {
                return new List<string>(ExtractValuesList(s, "[", "]"));
            }

            internal static List<MonetDBColumnInfo> GetColumnInfo(List<string> header_info)
            {
                List<MonetDBColumnInfo> list = new List<MonetDBColumnInfo>();
                foreach (string s in header_info)
                {
                    KeyValuePair<string, List<string>> r = SplitColumnInfoLine(s);
                    if (list.Count == 0)
                        foreach (string ci in r.Value)
                            list.Add(new MonetDBColumnInfo());

                    for (int i = 0; i < r.Value.Count; i++)
                    {
                        switch (r.Key)
                        {
                            case "table_name":
                                list[i].tableName = r.Value[i];
                                break;
                            case "name":
                                list[i].name = r.Value[i];
                                break;
                            case "type":
                                list[i].dataType = r.Value[i];
                                break;
                            case "length":
                                list[i].length = int.Parse(r.Value[i]);
                                break;
                        }
                    }
                }
                return list;
            }


            public IEnumerable<MonetDBQueryResponseInfo> GetResults()
            {
                _temp = _stream.ReadLine();
                do
                {
                    MonetDBQueryResponseInfo ri = new MonetDBQueryResponseInfo();
                    List<string> header_info = new List<string>();
                    while (_temp != "." && _temp[0] != '[')
                    {
                        switch (_temp[0])
                        {
                            case '&':
                                ri = GetQueryResponseInfo(_temp);
                                break;
                            case '%':
                                header_info.Add(_temp);
                                break;
                            case '!':
                                throw new MonetDbException("Error! " + _temp.Substring(1));
                        }
                        _temp = _stream.ReadLine();
                    }

                    ri.columns = GetColumnInfo(header_info);
                    ri.data = GetRows();
                    yield return ri;
                } while (_temp != ".");
                _stream = null;
            }
        }


        /// <summary>
        /// The MonetDB server has it's own protocol for streaming chunked input and output.
        /// This is known as the "block" stream.  
        /// 
        /// A byte stream to and from the MonetDB server consists of one or more "blocks".
        /// A block is a sequence of bytes, with the first two bytes indicating a 16-bit
        /// integer length followed by the length number of bytes of data.  This can go on
        /// for as many blocklength+block series are sent from the server, and the end of a 
        /// sequence is indicated by a block with the most significant big set to 1 (blockHeader[0] &amp; 0x1) == 1).
        /// 
        /// When reading from the stream we end the sequence with a \n.\n (the first \n is added if not sent
        /// by the server).  This makes this class trivial to wrap with a StreamReader and StreamWriter.
        /// 
        /// When writing to the server, we write the terminating block header
        /// when the Flush() function is called.  If that's not called, we write out
        /// blocks to the server as they're filled.
        /// </summary>
        private class MonetDbStream : Stream
        {
            private Stream _monetStream;
            private byte[] _readBlock = new byte[Int16.MaxValue + 3];
            private byte[] _writeBlock = new byte[Int16.MaxValue];
            private int _readPos, _writePos, _readLength, _writeLength;
            private bool _lastReadBlock;

            public MonetDbStream(Stream monetStream)
            {
                _monetStream = new BufferedStream(monetStream);
                _lastReadBlock = false;
            }

            public override bool CanRead
            {
                get { return _monetStream.CanRead; }
            }

            public override bool CanSeek
            {
                get { return _monetStream.CanSeek; }
            }

            public override bool CanWrite
            {
                get { return _monetStream.CanWrite; }
            }

            public override void Flush()
            {
                WriteNextBlock(true);
            }

            public override long Length
            {
                get { return _monetStream.Length; }
            }

            public override long Position
            {
                get { return _monetStream.Position; }
                set { _monetStream.Position = value; }
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                if (offset + count > buffer.Length)
                    throw new ArgumentOutOfRangeException("offset", "offset + count cannot be greater than the size of the buffer");
                int available = _readLength - _readPos;
                int retval = 0;
                if (available == 0)
                    available = ReadNextBlock();
                while (available > 0 && retval < count)
                {
                    int length = count - retval > available ? available : count - retval;
                    Array.Copy(_readBlock, _readPos, buffer, offset, length);
                    retval += length;
                    offset += length;
                    _readPos += length;
                    available = _readLength - _readPos;
                    if (!_lastReadBlock && available == 0)
                        available = ReadNextBlock();
                }

                return retval;
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new InvalidOperationException("Seeking not allowed on a network based stream");
            }

            public override void SetLength(long value)
            {
                throw new InvalidOperationException("SetLength is not valid on a network based stream");
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                if (offset + count > buffer.Length)
                    throw new ArgumentOutOfRangeException("offset", "offset + count cannot be greater than the buffer length");
                while (count > 0)
                {
                    if (count < _writeBlock.Length - _writePos)
                    {
                        //in this case we won't fill up the block buffer so we can just copy the bytes
                        //to the buffer.
                        Array.Copy(buffer, offset, _writeBlock, _writePos, count);
                        _writeLength += count;
                        count = 0;
                    }
                    else
                    {
                        //In this case we will fill up the block buffer, so we need to copy
                        //what we can to the block buffer, write it out, and write what's left
                        int tempCount = _writeBlock.Length - _writePos;
                        Array.Copy(buffer, offset, _writeBlock, _writePos, tempCount);
                        offset += tempCount;
                        count -= tempCount;
                        _writeLength += tempCount;
                        WriteNextBlock(false);
                    }
                }
            }

            /// <summary>
            /// Reads the next available block on the provided stream.  Returns the bytes available in the block buffer.
            /// </summary>
            private int ReadNextBlock()
            {
                byte[] blockHeader = new byte[2];
                if (_monetStream.Read(blockHeader, 0, blockHeader.Length) != 2)
                    throw new MonetDbException(new InvalidDataException("Invalid block header length"), "Error reading data from MonetDB server");
                _readLength = ((blockHeader[0] & 0xFF) >> 1 | (((short)blockHeader[1] & 0xFF) << 7));
                _lastReadBlock = (blockHeader[0] & 0x1) == 1;
                int read = 0;
                while (read < _readLength)
                    read += _monetStream.Read(_readBlock, read, _readLength - read);
                _readPos = 0;
                if (_lastReadBlock)
                {
                    if (_readLength > 0 && _readBlock[_readLength - 1] != '\n')
                        _readBlock[_readLength++] = (byte)'\n';
                    _readBlock[_readLength++] = (byte)MonetDbLineType.Prompt;
                    _readBlock[_readLength++] = (byte)'\n';
                }

                return _readLength - _readPos;
            }

            /// <summary>
            /// Writes the next block to the provided stream.
            /// </summary>
            /// <param name="last">If <c>true</c> then we should write out the block header to indicate that this is the end
            /// of the sequence.  If <c>false</c> the the server should expect more data.</param>
            private void WriteNextBlock(bool last)
            {
                byte[] blockHeader = new byte[2];
                Int16 blockSize = (Int16)_writeLength;
                //if this is the last block then we set the most significant bit to 1
                //if this is not the last block then we set the most significant bit to 0
                if (last)
                {
                    blockHeader[0] = (byte)(blockSize << 1 & 0xFF | 1);
                    blockHeader[1] = (byte)(blockSize >> 7);
                }
                else
                {
                    blockHeader[0] = (byte)(blockSize << 1 & 0xFF);
                    blockHeader[1] = (byte)(blockSize >> 7);
                }

                _monetStream.Write(blockHeader, 0, blockHeader.Length);
                _monetStream.Write(_writeBlock, 0, _writeLength);
                _monetStream.Flush();
                _writePos = 0;
                _writeLength = 0;
            }
        }
    }
}
