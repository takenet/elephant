using System;
using Takenet.Elephant.Memory;
using Takenet.Elephant.Tests.Specialized;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryGuidItemScopedMapFacts : GuidItemScopedMapFacts
    {
        public override IMap<Guid, Item> CreateMap()
        {
            return new Map<Guid, Item>();
        }

        public override ISetMap<string, string> CreateKeysSetMap()
        {
            return new SetMap<string, string>();
        }

        public override ISerializer<Guid> CreateKeySerializer()
        {
            return new GuidSerializer();
        }
    }
}