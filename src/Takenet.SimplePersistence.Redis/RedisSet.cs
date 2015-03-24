using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Redis
{
    /// <summary>
    /// Implements the <see cref="ISet{T}"/> interface using Redis set data structure.
    /// </summary>
    /// <typeparam name="T"></typeparam>    
    public class RedisSet<T> : StorageBase<string>, ISet<T>
    {
        private readonly ISerializer<T> _serializer;

        public RedisSet(string setName, ISerializer<T> serializer, string configuration)
            : base(setName, configuration)
        {
            if (serializer == null) throw new ArgumentNullException(nameof(serializer));            
            _serializer = serializer;
        }

        #region ISet<T> Members

        public async Task AddAsync(T value)
        {
            var database = _connectionMultiplexer.GetDatabase();
            await database.SetAddAsync(_name, _serializer.Serialize(value));
        }

        public Task<bool> TryRemoveAsync(T value)
        {
            var database = _connectionMultiplexer.GetDatabase();
            return database.SetRemoveAsync(_name, _serializer.Serialize(value));
        }

        public async Task<IAsyncEnumerable<T>> AsEnumerableAsync()
        {
            var database = _connectionMultiplexer.GetDatabase();
            var values = await database.SetMembersAsync(_name);
            return new AsyncEnumerableWrapper<T>(values.Select(value => _serializer.Deserialize(value)).ToList());
        }

        public Task<bool> ContainsAsync(T value)
        {
            var database = _connectionMultiplexer.GetDatabase();
            return database.SetContainsAsync(_name, _serializer.Serialize(value));
        }

        #endregion
    }
}
