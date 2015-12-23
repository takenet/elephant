using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.Elephant.Redis
{
    /// <summary>
    /// Implements the <see cref="ISet{T}"/> interface using Redis set data structure.
    /// </summary>
    /// <typeparam name="T"></typeparam>    
    public class RedisSet<T> : StorageBase<string>, ISet<T>
    {
        private readonly ISerializer<T> _serializer;
        private readonly bool _useScanOnEnumeration;

        public RedisSet(string setName, string configuration, ISerializer<T> serializer, int db = 0, bool useScanOnEnumeration = true)
            : this(setName, ConnectionMultiplexer.Connect(configuration), serializer, db, useScanOnEnumeration)
        {
            
        }

        public RedisSet(string setName, ConnectionMultiplexer connectionMultiplexer, ISerializer<T> serializer, int db = 0, bool useScanOnEnumeration = true)
            : base(setName, connectionMultiplexer, db)
        {
            if (serializer == null) throw new ArgumentNullException(nameof(serializer));
            _serializer = serializer;
            _useScanOnEnumeration = useScanOnEnumeration;
        }

        #region ISet<T> Members

        public Task AddAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var database = GetDatabase();
            return database.SetAddAsync(_name, _serializer.Serialize(value));
        }

        public Task<bool> TryRemoveAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var database = GetDatabase();
            return database.SetRemoveAsync(_name, _serializer.Serialize(value));
        }

        public async Task<IAsyncEnumerable<T>> AsEnumerableAsync()
        {
            var database = GetDatabase() as IDatabase;
            if (database == null) throw new NotSupportedException("The database instance is not supported");
            
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
            var database = GetDatabase();
            return database.SetContainsAsync(_name, _serializer.Serialize(value));
        }

        public Task<long> GetLengthAsync()
        {
            var database = GetDatabase();
            return database.SetLengthAsync(_name);
        }

        #endregion
    }
}
