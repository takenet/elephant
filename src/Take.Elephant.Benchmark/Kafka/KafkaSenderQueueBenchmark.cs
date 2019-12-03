using AutoFixture;
using BenchmarkDotNet.Attributes;
using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Kafka;

namespace Take.Elephant.Benchmark.Kafka
{
    [SimpleJob(BenchmarkDotNet.Jobs.RuntimeMoniker.NetCoreApp30)]
    [MemoryDiagnoser]
    [ThreadingDiagnoser]
    [RankColumn]
    public class KafkaSenderQueueBenchmark
    {
        private IEnumerable<Item> items;
        private KafkaSenderQueue<Item> sender;
        private CancellationToken cancellationToken;

        [Params(250000)]
        public int itemsCount;

        [Params(6000, 12000, 24000)]
        public int lingerMs;

        [GlobalSetup]
        public void Setup()
        {
            var fixture = new Fixture();
            items = Enumerable.Range(0, itemsCount).Select(i => fixture.Create<Item>());

            var fqdn = "";
            var connectionString = "";
            var caCertPath = Path.Combine(Environment.CurrentDirectory, "Kafka", "cacert.pem");
            var clientConfig = new ClientConfig
            {
                BootstrapServers = fqdn,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = connectionString,
                SslCaLocation = caCertPath,
            };
            var producerConfig = new ProducerConfig(clientConfig)
            {
                LingerMs = lingerMs,
                BatchNumMessages = 100000
            };
            sender = new KafkaSenderQueue<Item>(producerConfig, "items_benchmark", new JsonItemSerializer());
            var cts = new CancellationTokenSource();
            cancellationToken = cts.Token;
            cts.CancelAfter(10000);
        }

        [Benchmark]
        public async Task ProduceItems() => 
            await Task.WhenAll(items.Select(i => sender.EnqueueAsync(i, cancellationToken)));
    }
}