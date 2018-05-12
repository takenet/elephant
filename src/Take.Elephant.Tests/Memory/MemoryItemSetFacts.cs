using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
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