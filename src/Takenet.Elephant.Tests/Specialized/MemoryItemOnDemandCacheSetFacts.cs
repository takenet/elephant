using Takenet.Elephant.Memory;
using Takenet.Elephant.Redis;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;
using Takenet.Elephant.Tests.Redis;
using Xunit;

namespace Takenet.Elephant.Tests.Specialized
{
    [Trait("Category", nameof(Memory))]
    public class MemoryItemOnDemandCacheSetFacts : ItemOnDemandCacheSetFacts
    {
        public override ISet<Item> CreateSource() => new Set<Item>();

        public override ISet<Item> CreateCache() => new Set<Item>();
    }
}