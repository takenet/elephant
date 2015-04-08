using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NFluent;

namespace Takenet.SimplePersistence.Tests
{
    public abstract class QueueMapFacts<TKey, TValue> : MapFacts<TKey, IQueue<TValue>>
    {
        public override void AssertEquals<T>(T actual, T expected)
        {
            if (typeof(IQueue<TValue>).IsAssignableFrom(typeof(T)) &&
                actual != null && expected != null)
            {
                var actualQueue = (IQueue<TValue>)actual;
                var expectedQueue = (IQueue<TValue>)expected;                                
                var actualValues = GetQueueItems<T>(actualQueue);
                var expectedValues = GetQueueItems<T>(expectedQueue);
                Check.That(actualValues).Contains(expectedValues);
            }
            else
            {
                base.AssertEquals(actual, expected);
            }
        }

        private static List<TValue> GetQueueItems<T>(IQueue<TValue> actualQueue)
        {
            var values = new List<TValue>();
            
            while (true)
            {
                var value = actualQueue.DequeueOrDefaultAsync().Result;
                if (value == null || value.Equals(default(TValue)))
                {
                    break;
                }
                values.Add(value);
            }

            return values;
        }
    }
}