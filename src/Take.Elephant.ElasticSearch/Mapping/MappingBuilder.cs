using System;
using System.Collections.Generic;
using System.Text;

namespace Take.Elephant.Elasticsearch.Mapping
{
    public sealed class MappingBuilder
    {
        /// <summary>
        /// Includes the document field to be used as key
        /// </summary>
        /// <param name="keyField"></param>
        /// <returns></returns>
        public MappingBuilder WithKeyField(string keyField)
        {
            KeyField = keyField;
            return this;
        }

        private MappingBuilder(string index)
        {
            Index = index;
        }

        /// <summary>
        /// Gets the document Index
        /// </summary>
        public string Index { get; set; }

        /// <summary>
        /// Gets the key field
        /// </summary>
        public string KeyField { get; set; }

        /// <summary>
        /// Includes the document index name
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public static MappingBuilder WithIndex(string index)
        {
            return new MappingBuilder(index);
        }

        /// <summary>
        /// Creates an IMapping instance based on the provided parameters
        /// </summary>
        /// <returns></returns>
        public IMapping Build()
        {
            return new Mapping(Index, KeyField);
        }
    }
}
