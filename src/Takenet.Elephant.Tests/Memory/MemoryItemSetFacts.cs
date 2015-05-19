using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryItemSetFacts : ItemSetFacts
    {
        public override ISet<Item> Create()
        {
            return new Set<Item>();
        }
    }
}