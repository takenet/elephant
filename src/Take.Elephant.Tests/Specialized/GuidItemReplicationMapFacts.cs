using System;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    [Trait("Category", nameof(Specialized))]
    public class GuidItemReplicationMapFacts : ReplicationMapFacts<Guid, Item>
    {

    }
}