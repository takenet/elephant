using System;
using Takenet.Elephant.Memory;
using Takenet.Elephant.Specialized.Scoping;
using Takenet.Elephant.Tests.Specialized;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryGuidItemScopedMapFacts : GuidItemScopedMapFacts
    {
        public override IMap<Guid, Item> CreateMap()
        {
            return new Map<Guid, Item>();
        }

        public override ISetMap<string, IdentifierKey> CreateKeysSetMap()
        {
            return new SetMap<string, IdentifierKey>();
        }

        public override ISerializer<Guid> CreateKeySerializer()
        {
            return new GuidSerializer();
        }
    }
}