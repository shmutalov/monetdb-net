using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;

namespace MonetDb
{
    /// <summary>
    /// Exception type of all MonetDB related errors.
    /// </summary>
    [Serializable]
    public class MonetDbException : DbException
    {
        public MonetDbException(string message): base(message)
        { }

        public MonetDbException(MonetDbException innerException, string message)
            : base(message, innerException)
        { }
    }
}
