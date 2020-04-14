using System;

namespace Take.Elephant.Tests.Specialized
{
    public abstract class GuidItemDistributedCacheMapFacts : DistributedCacheMapFacts<Guid, Item>
    {
        public override string CreateSynchronizationChannel() => "guid-items";
    }
}