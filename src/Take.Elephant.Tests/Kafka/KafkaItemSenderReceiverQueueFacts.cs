using Confluent.Kafka;
using System;
using System.IO;
using System.Linq;
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
            var consumerConfig = new ConsumerConfig(clientConfig)
            {
                GroupId = "default"
            };

            var consumer = new ConsumerBuilder<Ignore, Item>(consumerConfig)
                .SetValueDeserializer(new JsonDeserializer<Item>())
                .Build();
            var adminClient = new AdminClientBuilder(clientConfig).Build();

            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
            var topicMetada = metadata.Topics.First(t => t.Topic == topic);
            foreach (var partition in topicMetada.Partitions)
            {
                var topicPartition = new TopicPartition(topic, new Partition(partition.PartitionId));
                var offSet = consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(5));
                consumer.Commit(new TopicPartitionOffset[] { new TopicPartitionOffset(topicPartition, offSet.High) });
            }
            consumer.Close();

            var senderQueue = new KafkaSenderQueue<Item>(new ProducerConfig(clientConfig), topic, new JsonSerializer<Item>());
            var receiverQueue = new KafkaReceiverQueue<Item>(consumerConfig, topic, new JsonDeserializer<Item>());
            return (senderQueue, receiverQueue);
        }
    }
}