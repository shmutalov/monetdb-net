namespace System.Data.MonetDb.Models
{
    /// <summary>
    /// Column description (used when retreiving meta data)
    /// </summary>
    internal class MonetDbColumnInfoModel
    {
        /// <summary>
        /// Catalog name {may be null}
        /// </summary>
        public string Catalog { get; set; }

        /// <summary>
        /// Schema name (may be null)
        /// </summary>
        public string Schema { get; set; }

        /// <summary>
        /// Table name
        /// </summary>
        public string Table { get; set; }

        /// <summary>
        /// Column name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Data type name
        /// </summary>
        public string DataType { get; set; }

        /// <summary>
        /// Column size. For char or date types 
        /// this is the maximum number of characters, 
        /// for numeric or decimal types this is precision.
        /// </summary>
        public int ColumnSize { get; set; }

        /// <summary>
        /// The number of fractional digits
        /// </summary>
        public int Scale { get; set; }

        /// <summary>
        /// Radix (typically either 10 or 2)
        /// </summary>
        public int Radix { get; set; }

        /// <summary>
        /// Is null allowed?
        /// </summary>
        public bool Nullable { get; set; }

        /// <summary>
        /// Comment describing column (may be null)
        /// </summary>
        public string Remarks { get; set; }

        /// <summary>
        /// Column default value (may be null)
        /// </summary>
        public string DefaultValue { get; set; }

        /// <summary>
        /// Maximum number of bytes for char columns
        /// </summary>
        public int CharOctetLength { get; set; }

        /// <summary>
        /// Column ordinal position
        /// </summary>
        public int Ordinal { get; set; }
    }
}
