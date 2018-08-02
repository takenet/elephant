using Ploeh.AutoFixture;
using Take.Elephant.Memory;

namespace Take.Elephant.Tests.Memory
{
    public class MemoryIntegerStringListMapFacts : IntegerStringListMapFacts
    {
        public override IMap<int, IList<string>> Create()
        {
            return new ListMap<int, string>();
        }

        public override IList<string> CreateValue(int key, bool populate)
        {
            var list = new List<string>();
            if (populate)
            {
                list.AddAsync(Fixture.Create<string>()).Wait();
                list.AddAsync(Fixture.Create<string>()).Wait();
                list.AddAsync(Fixture.Create<string>()).Wait();
            }
            return list;
        }
    }
}
