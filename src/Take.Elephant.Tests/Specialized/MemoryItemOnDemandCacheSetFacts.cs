using Take.Elephant.Memory;
using Take.Elephant.Redis;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;
using Take.Elephant.Tests.Redis;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    [Trait("Category", nameof(Memory))]
    public class MemoryItemOnDemandCacheSetFacts : ItemOnDemandCacheSetFacts
    {
        public override ISet<Item> CreateSource() => new Set<Item>();

        public override ISet<Item> CreateCache() => new Set<Item>();
    }
}