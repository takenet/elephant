using System;
using System.Threading.Tasks;
using OpenSearch.Client;
using Take.Elephant.OpenSearch;
using Take.Elephant.OpenSearch.Mapping;
using Take.Elephant.Tests.DocumentOrientedSearch;
using Xunit;

namespace Take.Elephant.Tests.OpenSearch
{
    [Trait("Category", nameof(OpenSearch))]
    public class OpenSearchItemSetFacts : ItemSetFacts
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
                    new OpenSearchSet<Item>(
                        new OpenSearchClient(settings), mapping), 1000);
        }

        [Fact(Skip = "OpenSearch doesn't implement a lazy IEnumerable, so the AsEnumerableAsync method will return a snapshot of the index.")]
        public override Task EnumerateAfterRemovingItemsSucceeds()
        {
            return base.EnumerateAfterRemovingItemsSucceeds();
        }
    }
}