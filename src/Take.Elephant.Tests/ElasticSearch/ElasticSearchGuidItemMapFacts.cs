using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Take.Elephant.Elasticsearch;
using Take.Elephant.Elasticsearch.Mapping;
using Xunit;

namespace Take.Elephant.Tests.Elasticsearch
{
    [Trait("Category", nameof(Elasticsearch))]
    public class ElasticsearchGuidItemMapFacts : GuidItemMapFacts
    {
        public override IMap<Guid, Item> Create()
        {
            var mapping = MappingBuilder
                .WithIndex(Guid.NewGuid().ToString())
                .WithKeyField("id")
                .Build();

            var settings =
                new ConnectionSettings(new Uri("http://127.0.0.1:9200"))
                .DefaultIndex("tests");

            return new DelayedMapDecorator<Guid, Item>(
                    new ElasticsearchMap<Guid, Item>(
                        new ElasticClient(settings), mapping), 1000);
        }
    }
}
