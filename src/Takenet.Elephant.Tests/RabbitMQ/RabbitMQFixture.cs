using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Framing.Impl;

namespace Takenet.Elephant.Tests.RabbitMQ
{
    public class RabbitMQFixture : IDisposable
    {
        public RabbitMQFixture()
        {
            ConnectionFactory = new ConnectionFactory() { HostName = "localhost" };
        }

        internal IConnectionFactory ConnectionFactory { get;  }

        public void Dispose()
        {
        }
    }
}
