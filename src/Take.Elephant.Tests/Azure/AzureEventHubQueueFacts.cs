using Azure.Messaging.EventHubs.Consumer;
using System;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Azure;
using Xunit;

namespace Take.Elephant.Tests.Azure
{
    [Trait("Category", nameof(Azure))]
    public class AzureEventHubQueueFacts : ItemSenderReceiverQueueFacts
    {
        public override ValueTask<(ISenderQueue<Item>, IBlockingReceiverQueue<Item>)> CreateAsync(CancellationToken cancellationToken)
        {
            var connectionString = "";
            var topic = "teste";
            var consumerGroup = "$Default";
            var serializer = new JsonItemSerializer();
            // Changing the offset to the last value
            return new ValueTask<(ISenderQueue<Item>, IBlockingReceiverQueue<Item>)>((new AzureEventHubSenderQueue<Item>(connectionString, topic, serializer),
                new AzureEventHubReceiverQueue<Item>(connectionString, topic, consumerGroup, serializer)));
        }
    }
}