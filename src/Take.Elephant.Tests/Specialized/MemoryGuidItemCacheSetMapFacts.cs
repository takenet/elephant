using System;
using System.Data;
using System.Reflection;
using AutoFixture;
using Take.Elephant.Memory;
using Take.Elephant.Redis;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;
using Take.Elephant.Tests.Redis;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemCacheSetMapFacts : GuidItemCacheSetMapFacts
    {
        public override IMap<Guid, ISet<Item>> CreateSource() => new SetMap<Guid, Item>();

        public override IMap<Guid, ISet<Item>> CreateCache() => new SetMap<Guid, Item>();

        public override ISet<Item> CreateValue(Guid key, bool populate)
        {
            var set = new Set<Item>();
            if (populate)
            {
                set.AddAsync(Fixture.Create<Item>()).Wait();
                set.AddAsync(Fixture.Create<Item>()).Wait();
                set.AddAsync(Fixture.Create<Item>()).Wait();
            }
            return set;
        }
    }
}