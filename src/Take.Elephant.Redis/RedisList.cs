using StackExchange.Redis;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Take.Elephant.Redis
{
    /// <summary>
    /// Implements the <see cref="IPositionList{T}"/> interface using Redis set data structure.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class RedisList<T> : StorageBase<T>, IPositionList<T>
    {
        private readonly ISerializer<T> _serializer;

        public RedisList(string listName, string configuration, ISerializer<T> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None)
            : this(listName, StackExchange.Redis.ConnectionMultiplexer.Connect(configuration), serializer, db, readFlags, writeFlags)
        { }

        public RedisList(string listName, IConnectionMultiplexer connectionMultiplexer, ISerializer<T> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None)
            : base(listName, connectionMultiplexer, db, readFlags, writeFlags)
        {
            if (serializer == null) throw new ArgumentNullException(nameof(serializer));
            _serializer = serializer;
        }

        public Task AddAsync(T value)
        {
            var database = GetDatabase();
            return database.ListRightPushAsync(Name, _serializer.Serialize(value));
        }

        public async Task AddToPositionAsync(T value, int position = 0)
        {
            var database = GetDatabase();
            try
            {
                if (position == await database.ListLengthAsync(Name))
                {
                    await database.ListRightPushAsync(Name, _serializer.Serialize(value));
                }
                else
                {
                    var pivot = await database.ListGetByIndexAsync(Name, position);
                    await database.ListInsertBeforeAsync(Name, pivot, _serializer.Serialize(value));
                }
            }
            catch (ArgumentException e)
            {
                throw new ArgumentOutOfRangeException(e.Message, e);
            }
        }

        public async Task<IAsyncEnumerable<T>> AsEnumerableAsync()
        {
            var database = GetDatabase();
            var values = await database.ListRangeAsync(Name).ConfigureAwait(false);
            return new AsyncEnumerableWrapper<T>(values.Select(value => _serializer.Deserialize(value)));
        }

        public Task<long> GetLengthAsync()
        {
            var database = GetDatabase();
            return database.ListLengthAsync(Name);
        }

        public Task<long> RemoveAllAsync(T value)
        {
            var database = GetDatabase();
            return database.ListRemoveAsync(Name, _serializer.Serialize(value));
        }
    }
}