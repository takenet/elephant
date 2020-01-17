using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    [Trait("Category", nameof(Specialized))]
    public class ItemReplicationQueueFacts : ReplicationQueueFacts<Item>
    {
    }
}