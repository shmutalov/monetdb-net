namespace System.Data.MonetDb.Models
{
    /// <summary>
    /// Schema description (used when retreiving meta data)
    /// </summary>
    internal class MonetDbSchemaInfoModel
    {
        /// <summary>
        /// Schema catalog
        /// </summary>
        public string Catalog { get; set; }

        /// <summary>
        /// Schema name
        /// </summary>
        public string Name { get; set; }
    }
}
