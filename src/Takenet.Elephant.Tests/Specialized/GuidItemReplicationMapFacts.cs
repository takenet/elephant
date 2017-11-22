using System;
using Xunit;

namespace Takenet.Elephant.Tests.Specialized
{
    [Trait("Category", nameof(Specialized))]
    public class GuidItemReplicationMapFacts : ReplicationMapFacts<Guid, Item>
    {

    }
}