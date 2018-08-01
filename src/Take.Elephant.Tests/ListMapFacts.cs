using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Take.Elephant.Tests
{
    public abstract class ListMapFacts<TKey, TValue> : MapFacts<TKey, IList<TValue>>
    {
    }
}
