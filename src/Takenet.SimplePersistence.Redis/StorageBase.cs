using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.SimplePersistence.Redis
{
    public class StorageBase<TKey> : IDisposable
    {
        protected readonly ConnectionMultiplexer _connectionMultiplexer;
        protected readonly string _name;

        public StorageBase(string name, string configuration)
            : this(name, ConnectionMultiplexer.Connect(ConfigurationOptions.Parse(configuration)))
        {

        }

        protected StorageBase(string name, ConnectionMultiplexer connectionMultiplexer)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (connectionMultiplexer == null) throw new ArgumentNullException(nameof(connectionMultiplexer));
            _name = name;                        
            _connectionMultiplexer = connectionMultiplexer;
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
