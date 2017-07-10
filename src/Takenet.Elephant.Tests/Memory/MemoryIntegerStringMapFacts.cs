using Takenet.Elephant.Memory;
using Xunit;

namespace Takenet.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
    public class MemoryIntegerStringMapFacts : IntegerStringMapFacts
    {
        public override IMap<int, string> Create()
        {
            return new Map<int, string>();            
        }
    }
}
