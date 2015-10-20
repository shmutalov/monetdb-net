namespace MonetDB.Driver.Models
{
    /// <summary>
    /// Procedure description
    /// </summary>
    internal class MonetDbProcedureInfoModel
    {
        /// <summary>
        /// Catalog name {may be null)
        /// </summary>
        public string Catalog { get; set; }

        /// <summary>
        /// Schema name (may be null)
        /// </summary>
        public string Schema { get; set; }

        /// <summary>
        /// Procedure name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Reserver field (must be null)
        /// </summary>
        public object Field4 { get; set; }

        /// <summary>
        /// Reserver field (must be null)
        /// </summary>
        public object Field5 { get; set; }

        /// <summary>
        /// Reserver field (must be null)
        /// </summary>
        public object Filed6 { get; set; }

        /// <summary>
        /// Explanatory comment on the procedure
        /// </summary>
        public string Remarks { get; set; }

        /// <summary>
        /// Procedure type
        /// </summary>
        public int Type { get; set; }
    }
}
