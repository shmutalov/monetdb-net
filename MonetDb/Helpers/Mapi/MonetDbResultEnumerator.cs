using System.Collections.Generic;
using System.Data.MonetDb.Extensions;
using System.IO;
using System.Linq;

namespace System.Data.MonetDb.Helpers.Mapi
{
    /// <summary>
    /// This class process the stream into enumerated list of the MonetDBQueryResponseInfo objects which represent executed
    /// statements in the batch. IEnumerable is used to facilitate lazy execution and eliminate the need in materialization
    /// of the results returned by the server.
    /// </summary>
    internal class MonetDbResultEnumerator
    {
        private string _temp;
        private StreamReader _stream;

        public MonetDbResultEnumerator(StreamReader stream)
        {
            _stream = stream;
        }

        private IEnumerable<List<string>> GetRows()
        {
            while (_temp[0] == '[')
            {
                yield return SplitDataInColumns(_temp);
                _temp = _stream.ReadLine();

                if (_temp == null)
                    throw new IOException("Cannot read closed stream");
            }
        }

        private static IEnumerable<string> SplitCommaTabs(string s)
        {
            return s.Split(',').Select(v => v.Trim(' ', '\t'));
        }

        private static IEnumerable<string> ExtractValuesList(string s, string start, string end)
        {
            var startIndex = s.IndexOf(start, StringComparison.Ordinal);
            var endIndex = s.IndexOf(end, StringComparison.Ordinal);
            return SplitCommaTabs(s.Substring(startIndex + 1, endIndex - startIndex - 1));
        }

        private static KeyValuePair<string, List<string>> SplitColumnInfoLine(string s)
        {
            return new KeyValuePair<string, List<string>>(s.Substring(s.IndexOf('#') + 1).Trim(), new List<string>(ExtractValuesList(s, "%", "#")));
        }

        private static List<string> SplitDataInColumns(string s)
        {
            return new List<string>(ExtractValuesList(s, "[", "]"));
        }

        private static List<MonetDbColumnInfo> GetColumnInfo(List<string> headerInfo)
        {
            var list = new List<MonetDbColumnInfo>();
            var infoLines = headerInfo.Select(SplitColumnInfoLine);

            foreach (var infoLine in infoLines)
            {
                if (list.Count == 0)
                    list.AddRange(infoLine.Value.Select(ci => new MonetDbColumnInfo()));

                for (var i = 0; i < infoLine.Value.Count; i++)
                {
                    switch (infoLine.Key)
                    {
                        case "table_name":
                            list[i].TableName = infoLine.Value[i];
                            break;
                        case "name":
                            list[i].Name = infoLine.Value[i];
                            break;
                        case "type":
                            list[i].DataType = infoLine.Value[i];
                            break;
                        case "length":
                            list[i].Length = int.Parse(infoLine.Value[i]);
                            break;
                    }
                }
            }

            return list;
        }

        public IEnumerable<MonetDbQueryResponseInfo> GetResults()
        {
            _temp = _stream.ReadLine();

            if (_temp == null)
                throw new IOException("Unexpected end of stream");

            do
            {
                var ri = new MonetDbQueryResponseInfo();
                var headerInfo = new List<string>();

                while (_temp != "." && _temp[0] != '[')
                {
                    switch (_temp[0])
                    {
                        case '&':
                            ri = _temp.ToQueryResponseInfo();
                            break;
                        case '%':
                            headerInfo.Add(_temp);
                            break;
                        case '!':
                            throw new MonetDbException("Error! " + _temp.Substring(1));
                    }

                    _temp = _stream.ReadLine();

                    if (_temp == null)
                        throw new IOException("Unexpected end of stream");
                }

                ri.Columns = GetColumnInfo(headerInfo);
                ri.Data = GetRows();

                yield return ri;

            } while (_temp != ".");

            _stream = null;
        }
    }
}
