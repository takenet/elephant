using Take.Elephant.Memory;

namespace Take.Elephant.Tests.Memory
{
    public class MemoryStringItemBusFacts : StringItemBusFacts
    {
        public override IBus<string, Item> Create()
        {
            return new Bus<string, Item>();
        }
    }
}