using Confluent.Kafka;
using Take.Elephant.Kafka;
using Xunit;

namespace Take.Elephant.Tests.Kafka
{
    [Trait("Category", nameof(Kafka))]
    public class KafkaItemSenderReceiverQueueFacts : ItemSenderReceiverQueueFacts
    {
        public override (ISenderQueue<Item>, IBlockingReceiverQueue<Item>) Create()
        {
            var bootstrapServers = "localhost:9092";
            var topic = "items";

            var clientConfig = new ClientConfig
            {
                BootstrapServers = bootstrapServers,
            };

            var senderQueue = new KafkaSenderQueue<Item>(new ProducerConfig(clientConfig), topic, new JsonSerializer<Item>());
            var receiverQueue = new KafkaReceiverQueue<Item>(new ConsumerConfig(clientConfig) { GroupId = "default" }, topic, new JsonDeserializer<Item>());

            var value = new Item();
            while(value != default(Item))
            {
                var promise = receiverQueue.DequeueOrDefaultAsync();
                promise.Wait();
                value = promise.Result;
            }

            return (senderQueue, receiverQueue);
        }
    }
}