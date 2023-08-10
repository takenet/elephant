using System;
using OpenSearch.Client;
using Take.Elephant.OpenSearch;
using Take.Elephant.OpenSearch.Mapping;
using Take.Elephant.Tests.Search;
using Xunit;

namespace Take.Elephant.Tests.OpenSearch
{
    [Trait("Category", nameof(OpenSearch))]
    public class OpensearchGuidItemMapFacts : GuidItemMapFacts
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
                    new OpenSearchMap<Guid, Item>(
                        new OpenSearchClient(settings), mapping), 1000);
        }
    }
}