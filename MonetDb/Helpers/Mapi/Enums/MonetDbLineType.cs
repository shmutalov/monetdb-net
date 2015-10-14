namespace System.Data.MonetDb
{
    /// <summary>
    /// Represents the types of data sequences that can be returned from a MonetDB server.
    /// </summary>
    internal enum MonetDbLineType
    {
        /// <summary>
        /// "there is currently no line", or the the type is unknown
        /// </summary>
        Unknown = 0,
        /// <summary>
        /// A line starting with '!'
        /// </summary>
        Error = '!',
        /// <summary>
        /// A line starting with '%'
        /// </summary>
        Header = '%',
        /// <summary>
        /// A line starting with '['
        /// </summary>
        Result = '[',
        /// <summary>
        /// A line starting with '.'
        /// </summary>
        Prompt = '.',
        /// <summary>
        /// A line starting with ','
        /// </summary>
        More = ',',
        /// <summary>
        /// A line starting with '&amp;', indicating the start of a header block
        /// </summary>
        SoHeader = '&',
        /// <summary>
        /// A line starting with '^'
        /// </summary>
        Redirect = '^',
        /// <summary>
        /// A line starting with '#'
        /// </summary>
        Info = '#'
    }
}