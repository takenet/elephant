using Confluent.Kafka;
using System;
using System.Linq;
using Take.Elephant.Kafka;
using Take.Elephant.Tests.Azure;
using Xunit;

namespace Take.Elephant.Tests.Kafka
{
    [Trait("Category", nameof(Kafka))]
    public class KafkaItemSenderReceiverQueueFacts : ItemSenderReceiverQueueFacts
    {
        public override (IStreamSenderQueue<Item>, IBlockingReceiverQueue<Item>) Create()
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
                    BootstrapServers = bootstrapServers
                };
            }
            //Azure Event Hub
            else
            {
                var fqdn = "";
                var connectionString = "";
                clientConfig = new ClientConfig
                {
                    BootstrapServers = fqdn,
                    SecurityProtocol = SecurityProtocol.SaslSsl,
                    SaslMechanism = SaslMechanism.Plain,
                    SaslUsername = "$ConnectionString",
                    SaslPassword = connectionString,
                };
            }
            var consumerConfig = new ConsumerConfig(clientConfig)
            {
                GroupId = "default"
            };

            
            var adminClient = new AdminClientBuilder(clientConfig).Build();
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
            var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == topic);
            if (topicMetadata != null)
            {
                var consumer = new ConsumerBuilder<Ignore, Item>(consumerConfig)
                    .SetValueDeserializer(new JsonDeserializer<Item>())
                    .Build();

                foreach (var partition in topicMetadata.Partitions)
                {
                    var topicPartition = new TopicPartition(topic, new Partition(partition.PartitionId));
                    var offSet = consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(5));
                    consumer.Commit(new TopicPartitionOffset[] {new TopicPartitionOffset(topicPartition, offSet.High)});
                }
                consumer.Close();
            }
            
            var senderQueue = new KafkaSenderQueue<Item>(new ProducerConfig(clientConfig), topic, new JsonItemSerializer());
            var receiverQueue = new KafkaReceiverQueue<Item>(consumerConfig, topic, new JsonItemSerializer());
            receiverQueue.OpenAsync(CancellationToken).Wait();
            return (senderQueue, receiverQueue);
        }
    }
}