using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Memory
{
    public class NumberMap<TKey> : Map<TKey, long>, INumberMap<TKey>
    {
        public Task<long> IncrementAsync(TKey key)
        {
            return IncrementAsync(key, 1);
        }

        public Task<long> IncrementAsync(TKey key, long value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var updated = false;
            long updatedValue = 0;
            while (!updated)
            {
                var current = InternalDictionary.GetOrAdd(key, k => 0);
                updatedValue = current + value;
                updated = InternalDictionary.TryUpdate(key, updatedValue, current);
            }
            return updatedValue.AsCompletedTask();
        }

        public Task<long> DecrementAsync(TKey key)
        {
            return DecrementAsync(key, 1);
        }

        public Task<long> DecrementAsync(TKey key, long value)
        {
            return IncrementAsync(key, -1 * value);
        }
    }
}
