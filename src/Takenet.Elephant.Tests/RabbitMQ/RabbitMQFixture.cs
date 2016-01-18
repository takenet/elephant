using System;
using RabbitMQ.Client;

namespace Takenet.Elephant.Tests.RabbitMQ
{
    public class RabbitMQFixture : IDisposable
    {
        public RabbitMQFixture()
        {
            var factory = new ConnectionFactory()
            {
                HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOSTNAME"),
                VirtualHost = Environment.GetEnvironmentVariable("RABBITMQ_VHOST"),
                UserName = Environment.GetEnvironmentVariable("RABBITMQ_USERNAME"),
                Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD")
            };
            Connection = factory.CreateConnection();
        }

        internal IConnection Connection { get;  }

        public void Dispose()
        {
            Connection.Close();
        }
    }
}
