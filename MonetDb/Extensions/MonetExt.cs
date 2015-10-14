using System;
using System.Collections.Generic;
using System.Data.MonetDb.Helpers.Mapi;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace System.Data.MonetDb.Extensions
{
    internal static class MonetExt
    {
        public static MonetDbQueryResponseInfo ToQueryResponseInfo(this string info)
        {
            var sParts = info.Substring(1).Split(' ');

            switch (sParts[0])
            {
                case "1":
                case "5":
                    {
                        return new MonetDbQueryResponseInfo
                        {
                            Id = int.Parse(sParts[1]),
                            TupleCount = int.Parse(sParts[2]),
                            ColumnCount = int.Parse(sParts[3]),
                            RowCount = int.Parse(sParts[4])
                        }; ;
                    }
            }

            return new MonetDbQueryResponseInfo();
        }
    }
}
