using System.Collections.Generic;
using System.Threading.Tasks;
using NFluent;

namespace Take.Elephant.Tests
{
    public abstract class QueueMapFacts<TKey, TValue> : MapFacts<TKey, IQueue<TValue>>
    {
        public override async void AssertEquals<T>(T actual, T expected)
        {
            if (typeof(IQueue<TValue>).IsAssignableFrom(typeof(T)) &&
                actual != null && expected != null)
            {
                var actualQueue = (IQueue<TValue>)actual;
                var expectedQueue = (IQueue<TValue>)expected;                                
                var actualValues = await GetQueueItemsAsync(actualQueue);
                var expectedValues = await GetQueueItemsAsync(expectedQueue);
                Check.That(actualValues).Contains(expectedValues);
            }
            else
            {
                base.AssertEquals(actual, expected);
            }
        }

        private static async Task<List<TValue>> GetQueueItemsAsync(IQueue<TValue> actualQueue)
        {
            var values = new List<TValue>();
            
            while (true)
            {
                var value = await actualQueue.DequeueOrDefaultAsync();
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