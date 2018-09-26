using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Memory
{
    /// <summary>
    /// Simple memory bus implementation.
    /// </summary>
    /// <typeparam name="TChannel"></typeparam>
    /// <typeparam name="TMessage"></typeparam>
    public class Bus<TChannel, TMessage> : IBus<TChannel, TMessage>
    {
        public readonly ConcurrentDictionary<TChannel, ConcurrentBag<Func<TChannel, TMessage, CancellationToken, Task>>> _channelHandlerDictionary;

        public Bus()
        {
            _channelHandlerDictionary = new ConcurrentDictionary<TChannel, ConcurrentBag<Func<TChannel, TMessage, CancellationToken, Task>>>();
        }

        public virtual Task SubscribeAsync(TChannel channel, Func<TChannel, TMessage, CancellationToken, Task> handler, CancellationToken cancellationToken)
        {
            _channelHandlerDictionary.AddOrUpdate(
                channel,
                new ConcurrentBag<Func<TChannel, TMessage, CancellationToken, Task>>()
                {
                    handler
                },
                (_, handlers) =>
                {
                    handlers.Add(handler);
                    return handlers;
                });

            return Task.CompletedTask;
        }

        public virtual Task UnsubscribeAsync(TChannel channel, CancellationToken cancellationToken)
        {
            _channelHandlerDictionary.TryRemove(channel, out _);
            return Task.CompletedTask;
        }

        public virtual async Task PublishAsync(TChannel channel, TMessage message, CancellationToken cancellationToken)
        {
            if (!_channelHandlerDictionary.TryGetValue(channel, out var handlers)) return;

            await Task.WhenAll(handlers.Select(h => h(channel, message, cancellationToken)));
        }
    }
}
