using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Tests
{
    public abstract class KeyQueryableMapFacts<TKey, TValue> : FactsBase
    {
        public abstract Task<IKeyQueryableMap<TKey, TValue>> CreateAsync(params KeyValuePair<TKey, TValue>[] values);

        public abstract Expression<Func<TValue, bool>> CreateFilter(TValue value);
    }
}
