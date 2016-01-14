using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Framing.Impl;

namespace Takenet.Elephant.Tests.RabbitMQ
{
    public class RabbitMQFixture : IDisposable
    {
        public RabbitMQFixture()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            Connection = factory.CreateConnection();
        }


        internal IConnection Connection { get;  }

        public void Dispose()
        {
            Connection.Close();
        }
    }
}
