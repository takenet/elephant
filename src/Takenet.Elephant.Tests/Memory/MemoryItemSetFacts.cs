using Takenet.Elephant.Memory;
using Xunit;

namespace Takenet.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryItemSetFacts : ItemSetFacts
    {
        public override ISet<Item> Create()
        {
            return new Set<Item>();
        }
    }
}