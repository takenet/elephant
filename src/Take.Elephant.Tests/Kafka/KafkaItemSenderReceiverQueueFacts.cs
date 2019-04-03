using Take.Elephant.Kafka;
using Xunit;

namespace Take.Elephant.Tests.Kafka
{
    [Trait("Category", nameof(Kafka))]
    public class KafkaItemSenderReceiverQueueFacts : ItemSenderReceiverQueueFacts
    {
        public override (ISenderQueue<Item>, IReceiverQueue<Item>) Create()
        {
            var bootstrapServers = "kafka-944b4d2-andreb-67aa.aivencloud.com:12266";
            var topic = "items";
            var senderQueue = new KafkaSenderQueue<Item>(bootstrapServers, topic);
            var receiverQueue = new KafkaReceiverQueue<Item>(bootstrapServers, topic, "default");

            return (senderQueue, receiverQueue);
        }
    }
}