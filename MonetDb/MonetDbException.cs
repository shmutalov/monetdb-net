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
        /// <summary>
        /// Initializes a new exception which occurred with MonetDB.
        /// </summary>
        /// <param name="message">The message that the user should review and may help to determine what went wrong.</param>
        public MonetDbException(string message): base(message)
        { }

        /// <summary>
        /// Initializes a new exception which occurred with MonetDB.
        /// </summary>
        /// <param name="innerException">If this exception is wrapping another exception and throwing it up-level, this is the original exception.</param>
        /// <param name="message">The message that the user should review and may help to determine what went wrong.</param>
        public MonetDbException(MonetDbException innerException, string message)
            : base(message, innerException)
        { }
    }
}
