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
        public MonetDbException(Exception innerException, string message)
            : base(message, innerException)
        { }
    }
}
