using Ploeh.AutoFixture;
using Takenet.Elephant.Memory;
using Xunit;

namespace Takenet.Elephant.Tests.Memory
{
    [Trait("Category", nameof(Memory))]
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
