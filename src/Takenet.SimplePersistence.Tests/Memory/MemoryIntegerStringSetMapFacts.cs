using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Takenet.SimplePersistence.Memory;

namespace Takenet.SimplePersistence.Tests.Memory
{
    public class MemoryIntegerStringSetMapFacts : IntegerStringSetMapFacts
    {
        public override IMap<int, ISet<string>> Create()
        {
            return new DictionaryHashSetMap<int, string>();
        }

        public override ISet<string> CreateValue(int key)
        {
            var set = new SimplePersistence.Memory.HashSet<string>();
            set.AddAsync(Fixture.Create<string>()).Wait();
            set.AddAsync(Fixture.Create<string>()).Wait();
            set.AddAsync(Fixture.Create<string>()).Wait();
            return set;
        }
    }
}
