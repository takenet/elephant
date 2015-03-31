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
            Fixture.Register<ISet<string>>(() => new SimplePersistence.Memory.HashSet<string>());
            return new DictionaryHashSetMap<int, string>();
        }
    }
}
