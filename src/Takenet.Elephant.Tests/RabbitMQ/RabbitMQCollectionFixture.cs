using Xunit;

namespace Takenet.Elephant.Tests.RabbitMQ
{
    [CollectionDefinition("RabbitMQ")]
    public class RabbitMQCollectionFixture : ICollectionFixture<RabbitMQFixture>
    {
    }
}
