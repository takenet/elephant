using System;
using System.Threading.Tasks;
using Nest;
using Take.Elephant.Elasticsearch;
using Take.Elephant.Elasticsearch.Mapping;
using Take.Elephant.Tests.DocumentOrientedSearch;
using Xunit;

namespace Take.Elephant.Tests.Elasticsearch
{
    [Trait("Category", nameof(Elasticsearch))]
    public class ElasticsearchItemSetFacts : ItemSetFacts
    {
        public override ISet<Item> Create()
        {
            var mapping = MappingBuilder
                .WithIndex(Guid.NewGuid().ToString())
                .WithKeyField("GuidProperty")
                .Build();

            var settings =
                new ConnectionSettings(new Uri("http://127.0.0.1:9200"))
                .DefaultIndex("tests");

            return new DelayedSetDecorator<Item>(
                    new ElasticsearchSet<Item>(
                        new ElasticClient(settings), mapping), 1000);
        }

        [Fact(Skip = "Elasticsearch doesn't implement a lazy IEnumerable, so the AsEnumerableAsync method will return a snapshot of the index.")]
        public override Task EnumerateAfterRemovingItemsSucceeds()
        {
            return base.EnumerateAfterRemovingItemsSucceeds();
        }
    }
}