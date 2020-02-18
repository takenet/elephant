using Take.Elephant.Azure;
using Xunit;

namespace Take.Elephant.Tests.Azure
{
    [Trait("Category", nameof(Azure))]
    public class AzureEventHubQueueFacts : ItemSenderReceiverQueueFacts
    {
        public override (ISenderQueue<Item>, IBlockingReceiverQueue<Item>) Create()
        {
            var connectionString = "Endpoint=sb://hmg-msging1.servicebus.windows.net/;SharedAccessKeyName=teste;SharedAccessKey=ylbReehtbO5d+K9gjL/OydTb4QTWRIeip0epxXge2p4=";
            var topic = "funciona";
            var consumerGroup = "default";
            var serializer = new JsonItemSerializer();
            return (new AzureEventHubSenderQueue<Item>(topic, connectionString, serializer),
                new AzureEventHubReceiverQueue<Item>(connectionString, topic, consumerGroup, serializer));
        }
    }
}