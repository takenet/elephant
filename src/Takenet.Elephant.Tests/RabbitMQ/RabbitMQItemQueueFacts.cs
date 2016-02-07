using Takenet.Elephant.RabbitMQ;
using Xunit;

namespace Takenet.Elephant.Tests.RabbitMQ
{
    [Collection("RabbitMQ")]
    public class RabbitMQItemQueueFacts : ItemQueueFacts
    {
        private readonly RabbitMQFixture _rabbitMQFixture;

        public RabbitMQItemQueueFacts(RabbitMQFixture rabbitMQFixture)
        {
            _rabbitMQFixture = rabbitMQFixture;
        }

        public override IQueue<Item> Create()
        {
            const string setName = "items";
            return new RabbitMQQueue<Item>(setName, _rabbitMQFixture.ConnectionFactory, new ItemSerializer());
        }
    }
}
