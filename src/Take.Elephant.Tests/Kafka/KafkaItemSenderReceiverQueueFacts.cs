using Confluent.Kafka;
using System;
using System.IO;
using Take.Elephant.Kafka;
using Xunit;

namespace Take.Elephant.Tests.Kafka
{
    [Trait("Category", nameof(Kafka))]
    public class KafkaItemSenderReceiverQueueFacts : ItemSenderReceiverQueueFacts
    {
        public override (ISenderQueue<Item>, IBlockingReceiverQueue<Item>) Create()
        {
            ClientConfig clientConfig;
            var topic = "items";

            var localKafka = true;

            // Local Kafka
            if (localKafka)
            {
                var bootstrapServers = "localhost:9092";

                clientConfig = new ClientConfig
                {
                    BootstrapServers = bootstrapServers,
                };
            }
            //Azure Event Hub
            else
            {
                var fqdn = "";
                var connectionString = "";
                var caCertPath = Path.Combine(Environment.CurrentDirectory, "Kafka", "cacert.pem");
                clientConfig = new ClientConfig
                {
                    BootstrapServers = fqdn,
                    SecurityProtocol = SecurityProtocol.SaslSsl,
                    SaslMechanism = SaslMechanism.Plain,
                    SaslUsername = "$ConnectionString",
                    SaslPassword = connectionString,
                    SslCaLocation = caCertPath,
                };
            }

            var senderQueue = new KafkaSenderQueue<Item>(new ProducerConfig(clientConfig), topic, new JsonSerializer<Item>());
            var receiverQueue = new KafkaReceiverQueue<Item>(new ConsumerConfig(clientConfig) { GroupId = "default" }, topic, new JsonDeserializer<Item>());

            var value = new Item();
            while (value != default(Item))
            {
                var promise = receiverQueue.DequeueOrDefaultAsync();
                promise.Wait();
                value = promise.Result;
            }

            return (senderQueue, receiverQueue);
        }
    }
}