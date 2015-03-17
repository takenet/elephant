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
    public class MemoryIntegerObjectDictionaryMapFacts : IntegerObjectDictionaryMapFacts
    {
        public override IMap<int, object> Create()
        {
            return new DictionaryMap<int, object>();            
        }
    }
}
