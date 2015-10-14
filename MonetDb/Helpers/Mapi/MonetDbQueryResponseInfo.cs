using System.Collections.Generic;

namespace System.Data.MonetDb.Helpers.Mapi
{
    internal class MonetDbQueryResponseInfo
    {
        public int Id;
        public int ColumnCount;
        public int RowCount;
        public int TupleCount;
        public int RecordsAffected;
        public List<MonetDbColumnInfo> Columns;
        public IEnumerable<List<string>> Data;
    }
}