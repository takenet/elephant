using Take.Elephant.Azure;
using Xunit;

namespace Take.Elephant.Tests.Azure
{
    [Trait("Category", nameof(Azure))]
    public class AzureEventHubQueueFacts : ItemSenderReceiverQueueFacts
    {
        public override (ISenderQueue<Item>, IBlockingReceiverQueue<Item>) Create()
        {
            var connectionString = "";
            var topic = "";
            var consumerGroup = "default";
            var serializer = new JsonItemSerializer();
            return (new AzureEventHubSenderQueue<Item>(connectionString, topic, serializer),
                new AzureEventHubReceiverQueue<Item>(connectionString, topic, consumerGroup, serializer));
        }
    }
}