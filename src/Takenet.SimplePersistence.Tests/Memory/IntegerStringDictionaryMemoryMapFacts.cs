using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NFluent;
using Takenet.SimplePersistence.Memory;
using Xunit;
using Xunit.Extensions;

namespace Takenet.SimplePersistence.Tests.Memory
{
    public class MemoryDictionaryIntegerStringMapFacts : IntegerStringMapFacts
    {
        public override IMap<int, string> Create()
        {
            return new DictionaryMap<int, string>();            
        }
    }
}
