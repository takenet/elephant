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
        private readonly string _topic = "items";
        private static readonly ClientConfig _clientConfig = GetKafkaConfig();
        private static readonly ConsumerConfig _consumerConfig = new ConsumerConfig(_clientConfig) { GroupId = "default" };
        private KafkaSenderQueue<Item> _senderQueue;
        private KafkaReceiverQueue<Item> _receiverQueue;

        private static ClientConfig GetKafkaConfig()
        {
            ClientConfig clientConfig;
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
            return clientConfig;
        }

        public override (ISenderQueue<Item>, IBlockingReceiverQueue<Item>) Create()
        {
            var consumerConfig = new ConsumerConfig(_clientConfig)
            {
                GroupId = "default"
            };

            var adminClient = new AdminClientBuilder(_clientConfig).Build();
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
            var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == _topic);
            if (topicMetadata != null)
            {
                var consumer = new ConsumerBuilder<Ignore, Item>(consumerConfig)
                    .SetValueDeserializer(new JsonDeserializer<Item>())
                    .Build();

                foreach (var partition in topicMetadata.Partitions)
                {
                    var topicPartition = new TopicPartition(_topic, new Partition(partition.PartitionId));
                    var offSet = consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(5));
                    consumer.Commit(new TopicPartitionOffset[] {new TopicPartitionOffset(topicPartition, offSet.High)});
                }
                consumer.Close();
            }
            
            var senderQueue = new KafkaSenderQueue<Item>(new ProducerConfig(_clientConfig), _topic, new JsonItemSerializer());
            var receiverQueue = new KafkaReceiverQueue<Item>(consumerConfig, _topic, new JsonItemSerializer());
            receiverQueue.OpenAsync(CancellationToken).Wait();

            _senderQueue = senderQueue;
            _receiverQueue = receiverQueue;

            return (senderQueue, receiverQueue);
        }

        protected override void Dispose(bool disposing)
        {
            _senderQueue?.Dispose();
            _receiverQueue?.Dispose();
            base.Dispose(disposing);
        }
    }
}