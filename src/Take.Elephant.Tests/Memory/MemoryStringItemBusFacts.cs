using Take.Elephant.Memory;
using Xunit;

namespace Take.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryStringItemBusFacts : StringItemBusFacts
    {
        public override IBus<string, Item> Create()
        {
            return new Bus<string, Item>();
        }
    }
}