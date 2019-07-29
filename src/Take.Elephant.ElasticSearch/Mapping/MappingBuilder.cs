using System;
using System.Collections.Generic;
using System.Text;

namespace Take.Elephant.ElasticSearch.Mapping
{
    public sealed class MappingBuilder
    {
        /// <summary>
        /// Gets the document Index
        /// </summary>
        public string Index { get; set; }

        /// <summary>
        /// Gets the key field
        /// </summary>
        public string KeyField { get; set; }

        private MappingBuilder(string index)
        {
            Index = index;
        }

        public MappingBuilder WithIndex(string index)
        {
            return new MappingBuilder(index);
        }
        public MappingBuilder WithKeyField(string keyField)
        {
            KeyField = keyField;
            return this;
        }

        public IMapping Build()
        {
            return new Mapping(Index, KeyField);
        }
    }
}
