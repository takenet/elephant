using System;
using System.Collections.Generic;
using System.Text;

namespace Take.Elephant.ElasticSearch.Mapping
{
    public class Mapping : IMapping
    {
        /// <summary>
        /// Gets the document Index
        /// </summary>
        public string Index { get; set; }

        /// <summary>
        /// Gets the key field
        /// </summary>
        public string KeyField { get; set; }

        public Mapping(string index, string keyField)
        {
            Index = index;
            KeyField = keyField;
        }
    }
}
