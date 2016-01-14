using Takenet.Elephant.RabbitMQ;
using Xunit;

namespace Takenet.Elephant.Tests.RabbitMQ
{
    [Collection("RabbitMQ")]
    public class RabbitMQItemBlockingQueueFacts : ItemBlockingQueueFacts
    {
        private readonly RabbitMQFixture _rabitMQFixture;

        public RabbitMQItemBlockingQueueFacts(RabbitMQFixture rabitMQFixture)
        {
            _rabitMQFixture = rabitMQFixture;
        }

        public override IQueue<Item> Create()
        {
            const string setName = "items";
            return new RabbitMQQueue<Item>(setName, _rabitMQFixture.Connection, new ItemSerializer());
        }
    }
}
