using StackExchange.Redis;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Take.Elephant.Redis
{
    /// <summary>
    /// Implements the <see cref="IList{T}"/> interface using Redis set data structure.
    /// </summary>
    /// <typeparam name="T"></typeparam>   
    public class RedisList<T> : StorageBase<T>, IList<T>
    {

        private readonly ISerializer<T> _serializer;

        #region constructors

        public RedisList(string listName, string configuration, ISerializer<T> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None)
            : this(listName, StackExchange.Redis.ConnectionMultiplexer.Connect(configuration), serializer, db, readFlags, writeFlags)
        { }

        public RedisList(string listName, IConnectionMultiplexer connectionMultiplexer, ISerializer<T> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None)
            : base(listName, connectionMultiplexer, db, readFlags, writeFlags)
        {
            if (serializer == null) throw new ArgumentNullException(nameof(serializer));
            _serializer = serializer;
        }

        #endregion

        public Task AddAsync(T value)
        {
            var database = GetDatabase();
            return database.ListRightPushAsync(Name, _serializer.Serialize(value));
        }

        public async Task<IAsyncEnumerable<T>> AsEnumerableAsync()
        {
            var database = GetDatabase();
            var values = await database.ListRangeAsync(Name);
            return new AsyncEnumerableWrapper<T>(values.Select(value => _serializer.Deserialize(value)));
        }

        public Task<long> GetLengthAsync()
        {
            var database = GetDatabase();
            return database.ListLengthAsync(Name);
        }

        public async Task<long> RemoveAllAsync(T value)
        {
            var database = GetDatabase();
            return await database.ListRemoveAsync(Name, _serializer.Serialize(value));
        }
    }
}
