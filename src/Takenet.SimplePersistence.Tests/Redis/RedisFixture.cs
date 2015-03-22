using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.SimplePersistence.Tests.Redis
{
    public class RedisFixture : IDisposable
    {
        public RedisFixture()
        {
            var redisProcess = new ProcessStartInfo()
            {
                FileName = @"..\..\..\packages\Redis-64.2.8.19\redis-server.exe",
                CreateNoWindow = true,
                WindowStyle = ProcessWindowStyle.Hidden
            };

            ServerProcess = Process.Start(redisProcess);          
            var options = new ConfigurationOptions
            {
                EndPoints = { "localhost" },
                AllowAdmin = true,
                SyncTimeout = (int)TimeSpan.FromSeconds(2).TotalMilliseconds
            };
            Connection = ConnectionMultiplexer.Connect(options);            
            Server = Connection.GetServer(Connection.GetEndPoints().First());            
        }

        public Process ServerProcess { get; }
        public IServer Server { get; }
        public ConnectionMultiplexer Connection { get; }

        public void Dispose()
        {
            Connection.Dispose();
            ServerProcess.WaitForExit(500);
        }
    }
}
