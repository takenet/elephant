using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Framing.Impl;

namespace Takenet.Elephant.Tests.RabbitMQ
{
    public class RabbitMQFixture : IDisposable
    {
        public RabbitMQFixture()
        {
            //if (!File.Exists(@"..\..\..\packages\Redis-64.2.8.19\redis-server.exe"))
            //{
            //    throw new InvalidOperationException("Please install the 'redis-64' NuGet package with version '2.8.19' to run this test");
            //}

            //var redisProcess = new ProcessStartInfo()
            //{
            //    FileName = @"..\..\..\packages\Redis-64.2.8.19\redis-server.exe",
            //    Arguments = " --heapdir . --maxheap 100mb",
            //    CreateNoWindow = true,
            //    WindowStyle = ProcessWindowStyle.Hidden
            //};

            //ServerProcess = Process.Start(redisProcess);
            //var options = new ConfigurationOptions
            //{
            //    EndPoints = { "localhost" },
            //    AllowAdmin = true,
            //    SyncTimeout = (int)TimeSpan.FromSeconds(2).TotalMilliseconds
            //};
            //Connection = ConnectionMultiplexer.Connect(options);            
            //Server = Connection.GetServer(Connection.GetEndPoints().First());            
            //Connection = new Connection(, );
        }


        internal IConnection Connection { get;  }

        public void Dispose()
        {
            //Connection.Dispose();
            //ServerProcess.WaitForExit(500);
        }
    }
}
