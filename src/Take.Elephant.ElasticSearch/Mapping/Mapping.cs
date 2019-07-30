using System;
using System.Collections.Generic;
using System.Text;

namespace Take.Elephant.Elasticsearch.Mapping
{
    public class Mapping : IMapping
    {
        /// <summary>
        /// Creates a mapping based on index and key provided
        /// </summary>
        /// <param name="index"></param>
        /// <param name="keyField"></param>
        public Mapping(string index, string keyField)
        {
            Index = index;
            KeyField = keyField;
        }

        /// <summary>
        /// Gets the document Index
        /// </summary>
        public string Index { get; set; }

        /// <summary>
        /// Gets the key field
        /// </summary>
        public string KeyField { get; set; }
    }
}
