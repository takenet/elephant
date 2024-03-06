namespace Take.Elephant.Elasticsearch.Mapping
{
    public class Mapping : IMapping
    {
        /// <summary>
        /// Creates a mapping based on index and key provided
        /// </summary>
        /// <param name="index"></param>
        /// <param name="keyField"></param>
        public Mapping(string index, string type, string keyField)
        {
            Index = index;
            Type = type ?? "_doc";
            KeyField = keyField;
        }

        /// <summary>
        /// Gets the document Index
        /// </summary>
        public string Index { get; set; }

        /// <summary>
        /// Gets the type of document
        /// </summary>
        public string Type { get; set; }

        /// <summary>
        /// Gets the key field
        /// </summary>
        public string KeyField { get; set; }
    }
}
