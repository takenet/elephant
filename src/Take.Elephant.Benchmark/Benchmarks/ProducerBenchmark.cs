using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using Confluent.Kafka;
using Ploeh.AutoFixture;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Azure;
using Take.Elephant.Kafka;
using Take.Elephant.Tests;
using Take.Elephant.Tests.Azure;

namespace Take.Elephant.Benchmark.Benchmarks
{
    [SimpleJob(BenchmarkDotNet.Jobs.RuntimeMoniker.NetCoreApp31)]
    [HardwareCounters(HardwareCounter.TotalCycles, HardwareCounter.Timer)]
    [ThreadingDiagnoser]
    [MemoryDiagnoser]
    [RankColumn]
    public class ProducerBenchmark
    {
        private IEnumerable<Item> _items;
        private CancellationToken _cancellationToken;

        [Params(1000, 10000)]
        public int itemsCount;

        [GlobalSetup]
        public void Setup()
        {
            var fixture = new Fixture();
            _items = Enumerable.Range(0, itemsCount).Select(i => fixture.Create<Item>());
            var cts = new CancellationTokenSource();
            _cancellationToken = cts.Token;
            cts.CancelAfter(10000);
        }

        public ISenderQueue<Item> CreateKafkaSender()
        {
            var fqdn = "";
            var connectionString = "";
            var clientConfig = new ClientConfig
            {
                BootstrapServers = fqdn,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = connectionString,
            };
            var producerConfig = new ProducerConfig(clientConfig);
            return new KafkaSenderQueue<Item>(producerConfig, "", new JsonItemSerializer());
        }

        public ISenderQueue<Item> CreateEventHubSender()
        {
            var topic = "";
            var connectionString = "";
            return new AzureEventHubSenderQueue<Item>(connectionString, topic, new JsonItemSerializer());
        }

        [Benchmark]
        public async Task KafkaProduce()
        {
            var sender = CreateKafkaSender();
            await ProduceItems(sender);
        }

        [Benchmark]
        public async Task EventHubProduce()
        {
            var sender = CreateEventHubSender();
            await ProduceItems(sender);
        }

        public async Task ProduceItems(ISenderQueue<Item> sender)
            => await Task.WhenAll(_items.Select(i => sender.EnqueueAsync(i, _cancellationToken)));
    }
}