namespace System.Data.MonetDb.Models
{
    /// <summary>
    /// MonetDB table description (used when retreiving meta data)
    /// </summary>
    internal class MonetDbTableInfoModel
    {
        /// <summary>
        /// Table catalog (may be null)
        /// </summary>
        public string Catalog { get; set; }

        /// <summary>
        /// Table schema (may be null)
        /// </summary>
        public string Schema { get; set; }

        /// <summary>
        /// Table name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Table type
        /// </summary>
        /// <remarks>The valid values for the types parameter are:
        /// "TABLE", "INDEX", "SEQUENCE", "VIEW", 
        /// "SYSTEM TABLE", "SYSTEM INDEX", "SYSTEM VIEW", 
        /// "SYSTEM TOAST TABLE", "SYSTEM TOAST INDEX",
        /// "TEMPORARY TABLE", and "TEMPORARY VIEW"
        /// </remarks>
        public string Type { get; set; }

        /// <summary>
        /// Explanatory comment on the table
        /// </summary>
        public string Remarks { get; set; }
    }
}
