using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.SimplePersistence.Redis
{
    /// <summary>
    /// Implements the <see cref="ISet{T}"/> interface using Redis set data structure.
    /// </summary>
    /// <typeparam name="T"></typeparam>    
    public class RedisSet<T> : StorageBase<string>, ISet<T>
    {
        private readonly ISerializer<T> _serializer;
        private readonly bool _useScanOnEnumeration;

        public RedisSet(string setName, string configuration, ISerializer<T> serializer, bool useScanOnEnumeration = true)
            : base(setName, configuration)
        {
            if (serializer == null) throw new ArgumentNullException(nameof(serializer));            
            _serializer = serializer;
            _useScanOnEnumeration = useScanOnEnumeration;
        }

        protected RedisSet(string setName, ConnectionMultiplexer connectionMultiplexer, ISerializer<T> serializer, bool useScanOnEnumeration = true)
            : base(setName, connectionMultiplexer)
        {
            if (serializer == null) throw new ArgumentNullException(nameof(serializer));
            _serializer = serializer;
            _useScanOnEnumeration = useScanOnEnumeration;
        }

        #region ISet<T> Members

        public Task AddAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var database = _connectionMultiplexer.GetDatabase();
            return database.SetAddAsync(_name, _serializer.Serialize(value));
        }

        public Task<bool> TryRemoveAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var database = _connectionMultiplexer.GetDatabase();
            return database.SetRemoveAsync(_name, _serializer.Serialize(value));
        }

        public async Task<IAsyncEnumerable<T>> AsEnumerableAsync()
        {
            var database = _connectionMultiplexer.GetDatabase();
            IEnumerable<RedisValue> values;
            if (_useScanOnEnumeration)
            {
                values = database.SetScan(_name);
            }
            else
            {
                values = await database.SetMembersAsync(_name).ConfigureAwait(false);                
            }

            return new AsyncEnumerableWrapper<T>(values.Select(value => _serializer.Deserialize(value)));
        }

        public Task<bool> ContainsAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var database = _connectionMultiplexer.GetDatabase();
            return database.SetContainsAsync(_name, _serializer.Serialize(value));
        }

        #endregion
    }
}
