using System;
using StackExchange.Redis;

namespace Take.Elephant.Redis
{
    public class StorageBase<TKey> : IDisposable
    {
        private readonly bool _disposeMultiplexer;

        public StorageBase(string name, string configuration, int db, CommandFlags readFlags, CommandFlags writeFlags)
            : this(name, StackExchange.Redis.ConnectionMultiplexer.Connect(ConfigurationOptions.Parse(configuration)), db, readFlags, writeFlags, true)
        {

        }

        protected StorageBase(string name, IConnectionMultiplexer connectionMultiplexer, int db, CommandFlags readFlags, CommandFlags writeFlags, bool disposeMultiplexer = false)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            ConnectionMultiplexer = connectionMultiplexer ?? throw new ArgumentNullException(nameof(connectionMultiplexer));
            ConnectionMultiplexer.PreserveAsyncOrder = false;
            Db = db;
            ReadFlags = readFlags;
            WriteFlags = writeFlags;
            _disposeMultiplexer = disposeMultiplexer;
        }

        protected IConnectionMultiplexer ConnectionMultiplexer { get; }

        protected string Name { get; }

        protected int Db { get; }

        protected CommandFlags ReadFlags { get; }

        protected CommandFlags WriteFlags { get; }

        protected virtual string GetRedisKey(TKey key)
        {
            return GetRedisKey(KeyToString(key));
        }

        protected virtual string GetRedisKey(string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            return $"{Name}:{key}";
        }

        protected virtual TKey GetKeyFromString(string value)
        {
            if (typeof(TKey) == typeof(string))
                return (TKey)(object)value;
            return TypeUtil.GetParseFunc<TKey>()(value);
        }

        protected virtual string KeyToString(TKey key) => key.ToString();

        protected virtual IDatabaseAsync GetDatabase() => ConnectionMultiplexer.GetDatabase(Db);

        protected virtual ISubscriber GetSubscriber() => ConnectionMultiplexer.GetSubscriber();

        #region IDisposable Members

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_disposeMultiplexer)
                {
                    ConnectionMultiplexer.Dispose();
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion IDisposable Members
    }
}