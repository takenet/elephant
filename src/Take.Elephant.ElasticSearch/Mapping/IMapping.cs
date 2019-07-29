using System;
using System.Collections.Generic;
using System.Text;

namespace Take.Elephant.Elasticsearch.Mapping
{
    public interface IMapping
    {
        string Index { get; }
        string KeyField { get; }
    }
}
