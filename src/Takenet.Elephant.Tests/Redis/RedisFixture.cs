using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using StackExchange.Redis;

namespace Takenet.Elephant.Tests.Redis
{
    public class RedisFixture : IDisposable
    {
        public RedisFixture()
        {
            // Note: You should run a local redis server
            var options = new ConfigurationOptions
            {
                EndPoints =
                {
                    "localhost"
                },
                AllowAdmin = true,
                SyncTimeout = (int)TimeSpan.FromSeconds(2).TotalMilliseconds
            };
            Connection = ConnectionMultiplexer.Connect(options);            
            Server = Connection.GetServer(Connection.GetEndPoints().First());
        }

        public IServer Server { get; }
        public ConnectionMultiplexer Connection { get; }

        public void Dispose()
        {
            Connection.Dispose();
        }
    }
}
