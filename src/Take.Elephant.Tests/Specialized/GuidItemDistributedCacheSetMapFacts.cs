using System;

namespace Take.Elephant.Tests.Specialized
{
    public abstract class GuidItemDistributedCacheSetMapFacts : DistributedCacheSetMapFacts<Guid, Item>
    {
        public override string CreateSynchronizationChannel() => "guid-items";
    }
}