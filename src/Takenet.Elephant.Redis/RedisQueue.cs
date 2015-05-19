using System;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.Elephant.Redis
{
    public class RedisQueue<T> : StorageBase<T>, IQueue<T>
    {
        private readonly ISerializer<T> _serializer;    

        public RedisQueue(string queueName, string configuration, ISerializer<T> serializer)
            : base(queueName, configuration)
        {
            _serializer = serializer;
        }

        internal RedisQueue(string queueName, ConnectionMultiplexer connectionMultiplexer, ISerializer<T> serializer)
            : base(queueName, connectionMultiplexer)
        {
            _serializer = serializer;
        }

        #region IQueue<T> Members

        public Task EnqueueAsync(T item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            var database = GetDatabase();
            return database.ListLeftPushAsync(_name, _serializer.Serialize(item));
        }

        public async Task<T> DequeueOrDefaultAsync()
        {
            var database = GetDatabase();
            var result = await database.ListRightPopAsync(_name).ConfigureAwait(false);
            return !result.IsNullOrEmpty ? _serializer.Deserialize((string)result) : default(T);
        }

        public Task<long> GetLengthAsync()
        {
            var database = GetDatabase();
            return database.ListLengthAsync(_name);
        }

        #endregion
    }
}
