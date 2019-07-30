using System;
using System.Collections.Generic;
using System.Text;

namespace Take.Elephant.Elasticsearch.Mapping
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
        /// Elasticsearch document key field
        /// </summary>
        string KeyField { get; }
    }
}
