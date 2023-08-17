namespace Take.Elephant.OpenSearch.Mapping
{
    /// <summary>
    /// Opensearch document mapping
    /// </summary>
    public interface IMapping
    {
        /// <summary>
        /// Opensearch document inde
        /// </summary>
        string Index { get; }

        /// <summary>
        /// Opensearch document type
        /// </summary>
        string Type { get; }

        /// <summary>
        /// Opensearch document key field
        /// </summary>
        string KeyField { get; }
    }
}