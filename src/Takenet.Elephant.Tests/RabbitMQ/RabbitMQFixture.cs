using System;
using RabbitMQ.Client;

namespace Takenet.Elephant.Tests.RabbitMQ
{
    public class RabbitMQFixture : IDisposable
    {
        public RabbitMQFixture()
        {
            ConnectionFactory = new ConnectionFactory() 
            { 
                HostName = "localhost"
                //HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOSTNAME"),
                //VirtualHost = Environment.GetEnvironmentVariable("RABBITMQ_VHOST"),
                //UserName = Environment.GetEnvironmentVariable("RABBITMQ_USERNAME"),
                //Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD")
            };
        }

        public IConnectionFactory ConnectionFactory { get; private set; }

        public void Dispose()
        {
        }
    }
}
