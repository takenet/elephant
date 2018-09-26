using Ploeh.AutoFixture;
using System;
using Take.Elephant.Memory;

namespace Take.Elephant.Tests.Memory
{
    public class MemoryIntegerStringSortedSetMapFacts : IntegerStringSortedSetMapFacts
    {
        public override IMap<int, ISortedSet<string>> Create()
        {
            return new SortedSetMap<int, string>();
        }

        public override ISortedSet<string> CreateValue(int key, bool populate)
        {
            var list = new SortedSet<string>();
            if (populate)
            {
                list.AddAsync(Fixture.Create<string>(), Fixture.Create<double>()).Wait();
                list.AddAsync(Fixture.Create<string>(), Fixture.Create<double>()).Wait();
                list.AddAsync(Fixture.Create<string>(), Fixture.Create<double>()).Wait();
            }
            return list;
        }
    }
}