using System;
using StackExchange.Redis;

namespace Takenet.Elephant.Redis
{
    public class StorageBase<TKey> : IDisposable
    {        
        protected readonly ConnectionMultiplexer _connectionMultiplexer;
        protected readonly string _name;
        protected readonly int _db;

        public StorageBase(string name, string configuration, int db)
            : this(name, ConnectionMultiplexer.Connect(ConfigurationOptions.Parse(configuration)), db)
        {
            
        }

        protected StorageBase(string name, ConnectionMultiplexer connectionMultiplexer, int db)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (connectionMultiplexer == null) throw new ArgumentNullException(nameof(connectionMultiplexer));
            _name = name;                        
            _connectionMultiplexer = connectionMultiplexer;
            _db = db;
        }

        ~StorageBase()
        {
            Dispose(false);
        }

        protected virtual string GetRedisKey(TKey key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            return string.Format("{0}:{1}", _name, KeyToString(key));
        }

        protected virtual string KeyToString(TKey key)
        {
            return key.ToString();
        }

        protected virtual IDatabaseAsync GetDatabase()
        {
            return _connectionMultiplexer.GetDatabase(_db);
        }

        #region IDisposable Members

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _connectionMultiplexer.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
