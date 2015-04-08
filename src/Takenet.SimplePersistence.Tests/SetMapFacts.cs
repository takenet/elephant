using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NFluent;
using Xunit;

namespace Takenet.SimplePersistence.Tests
{
    public abstract class SetMapFacts<TKey, TValue> : MapFacts<TKey, ISet<TValue>>
    {
        public override void AssertEquals<T>(T actual, T expected)
        {
            if (typeof(ISet<TValue>).IsAssignableFrom(typeof(T)) &&
                actual != null && expected != null)
            {
                var actualSet = (ISet<TValue>) actual;
                var expectedSet = (ISet<TValue>)expected;
                Check.That(actualSet.AsEnumerableAsync().Result.ToListAsync().Result).Contains(expectedSet.AsEnumerableAsync().Result.ToListAsync().Result);
            }
            else
            {
                base.AssertEquals(actual, expected);
            }            
        }
    }
}
