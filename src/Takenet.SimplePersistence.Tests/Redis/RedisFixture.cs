using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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
            if (!File.Exists(@"..\..\..\packages\Redis-64.2.8.19\redis-server.exe"))
            {
                throw new InvalidOperationException("Please install the 'redis-64' with version '2.8.19' NuGet package to run this test");
            }

            var redisProcess = new ProcessStartInfo()
            {
                FileName = @"..\..\..\packages\Redis-64.2.8.19\redis-server.exe",
                Arguments = " --heapdir . --maxheap 100mb",
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
