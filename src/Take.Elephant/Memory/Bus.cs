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
        private readonly BusHandlerStore<TChannel, TMessage> _handlerStore;

        public Bus()
            : this(BusHandlerStore<TChannel, TMessage>.Default)
        {
            
        }

        public Bus(BusHandlerStore<TChannel, TMessage> handlerStore)
        {
            _handlerStore = handlerStore ?? throw new ArgumentNullException(nameof(handlerStore));
        }
        
        public virtual Task SubscribeAsync(TChannel channel, Func<TChannel, TMessage, CancellationToken, Task> handler, CancellationToken cancellationToken)
        {
            _handlerStore.ChannelHandlerDictionary.AddOrUpdate(
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
            _handlerStore.ChannelHandlerDictionary.TryRemove(channel, out _);
            return Task.CompletedTask;
        }

        public virtual async Task PublishAsync(TChannel channel, TMessage message, CancellationToken cancellationToken)
        {
            if (!_handlerStore.ChannelHandlerDictionary.TryGetValue(channel, out var handlers)) return;

            await Task.WhenAll(handlers.Select(h => h(channel, message, cancellationToken)));
        }
    }

    /// <summary>
    /// Implements a store for <see cref="Bus{TChannel,TMessage}"/> handlers.
    /// </summary>
    /// <typeparam name="TChannel"></typeparam>
    /// <typeparam name="TMessage"></typeparam>
    public sealed class BusHandlerStore<TChannel, TMessage>
    {
        public static BusHandlerStore<TChannel, TMessage> Default { get; } = new BusHandlerStore<TChannel, TMessage>();
        
        public BusHandlerStore()
        {
            ChannelHandlerDictionary = new ConcurrentDictionary<TChannel, ConcurrentBag<Func<TChannel, TMessage, CancellationToken, Task>>>();
        }
        
        public ConcurrentDictionary<TChannel, ConcurrentBag<Func<TChannel, TMessage, CancellationToken, Task>>> ChannelHandlerDictionary { get; }
    }
}
