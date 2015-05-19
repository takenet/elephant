using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryIntegerStringMapFacts : IntegerStringMapFacts
    {
        public override IMap<int, string> Create()
        {
            return new Map<int, string>();            
        }
    }
}
