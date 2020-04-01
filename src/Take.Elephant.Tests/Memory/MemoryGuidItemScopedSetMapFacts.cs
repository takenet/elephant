using System;
using AutoFixture;
using Take.Elephant.Memory;
using Take.Elephant.Specialized.Scoping;
using Take.Elephant.Tests.Specialized;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryGuidItemScopedSetMapFacts : GuidItemScopedSetMapFacts
    {
        public override IMap<Guid, ISet<Item>> CreateMap()
        {
            return new SetMap<Guid, Item>();
        }

        public override ISetMap<string, IdentifierKey> CreateKeysSetMap()
        {
            return new SetMap<string, IdentifierKey>();
        }

        public override ISerializer<Guid> CreateKeySerializer()
        {
            return new GuidSerializer();
        }

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