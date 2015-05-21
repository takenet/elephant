using Ploeh.AutoFixture;
using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Tests.Memory
{
    public class MemoryIntegerStringSetMapFacts : IntegerStringSetMapFacts
    {
        public override IMap<int, ISet<string>> Create()
        {
            return new SetMap<int, string>();
        }

        public override ISet<string> CreateValue(int key, bool populate)
        {
            var set = new Set<string>();
            if (populate)
            {
                set.AddAsync(Fixture.Create<string>()).Wait();
                set.AddAsync(Fixture.Create<string>()).Wait();
                set.AddAsync(Fixture.Create<string>()).Wait();
            }
            return set;
        }
    }
}
