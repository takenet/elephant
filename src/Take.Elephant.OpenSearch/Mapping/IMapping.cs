namespace Take.Elephant.OpenSearch.Mapping
{
    /// <summary>
    /// Elasticsearch document mapping
    /// </summary>
    public interface IMapping
    {
        /// <summary>
        /// Elasticsearch document inde
        /// </summary>
        string Index { get; }

        /// <summary>
        /// Elasticsearch document type
        /// </summary>
        string Type { get; }

        /// <summary>
        /// Elasticsearch document key field
        /// </summary>
        string KeyField { get; }
    }
}