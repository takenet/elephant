using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Take.Elephant.Kafka
{
    public class KafkaReceiverQueue<T> : IReceiverQueue<T>, IBlockingReceiverQueue<T>, ICloseable, IDisposable
    {
        private readonly IConsumer<string, T> _consumer;

        private readonly Task _consumerTask;
        private readonly CancellationTokenSource _cts;
        private readonly Channel<T> _channel;
        private bool _closed;

        public KafkaReceiverQueue(string bootstrapServers, string topic, string groupId)
        : this(new ConsumerConfig() { BootstrapServers = bootstrapServers, GroupId = groupId }, new[] { topic })
        {

        }

        public KafkaReceiverQueue(
            ConsumerConfig consumerConfig,
            IEnumerable<string> topics,
            IDeserializer<T> deserializer = null, 
            int bufferCapacity = 1)
        {
            var builder = new ConsumerBuilder<string, T>(consumerConfig);

            if (deserializer != null)
            {
                builder.SetValueDeserializer(deserializer);
            }

            _consumer = builder.Build();
            _consumer.Subscribe(topics);
            _cts = new CancellationTokenSource();
            _channel = bufferCapacity < 1 
                ? Channel.CreateUnbounded<T>() 
                : Channel.CreateBounded<T>(bufferCapacity);
            _consumerTask = Task.Factory.StartNew(
                () => ConsumeAsync(_cts.Token),
                TaskCreationOptions.LongRunning);
        }    
        
        public virtual Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            if (_channel.Reader.TryRead(out var item))
            {
                return item.AsCompletedTask();
            }

            return Task.FromResult(default(T));
        }

        public virtual Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            return _channel.Reader.ReadAsync(cancellationToken).AsTask();
        }
        
        public virtual Task CloseAsync(CancellationToken cancellationToken)
        {
            if (!_closed)
            {
                _consumer.Close();
                _cts.Cancel();
                _closed = true;
            }

            return _consumerTask;
        }

        private async Task ConsumeAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var result = _consumer.Consume(cancellationToken);
                    await _channel.Writer.WriteAsync(result.Value, cancellationToken);
                }
                catch (ConsumeException ex) when (!ex.Error.IsError)
                {
                    break;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }
            
            _channel.Writer.Complete();
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (!_cts.IsCancellationRequested) _cts.Cancel();
                _cts.Dispose();

                if (!_closed)
                {
                    _consumer.Close();
                }

                _consumer.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}