using StackExchange.Redis;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Redis
{
    /// <summary>
    /// Implementation of a <see cref="IBus{TChannel, TMessage}"/> using the Redis pub/sub interface.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class RedisBus<TKey, TValue> : StorageBase<TKey>, IBus<TKey, TValue>
    {
        private readonly string _channelNamePrefix;
        private readonly ISerializer<TValue> _serializer;
        private readonly TimeSpan _handlerTimeout;

        public RedisBus(
            string channelNamePrefix,
            string configuration,
            ISerializer<TValue> serializer,
            TimeSpan handlerTimeout = default,
            int db = 0,
            CommandFlags readFlags = CommandFlags.None,
            CommandFlags writeFlags = CommandFlags.None)
            : this(channelNamePrefix, StackExchange.Redis.ConnectionMultiplexer.Connect(configuration), serializer, handlerTimeout, db, readFlags, writeFlags, true)
        {

        }

        public RedisBus(
            string channelNamePrefix,
            IConnectionMultiplexer connectionMultiplexer,
            ISerializer<TValue> serializer,
            TimeSpan handlerTimeout = default,
            int db = 0,
            CommandFlags readFlags = CommandFlags.None,
            CommandFlags writeFlags = CommandFlags.None,
            bool disposeMultiplexer = false)
            : base(channelNamePrefix, connectionMultiplexer, db, readFlags, writeFlags, disposeMultiplexer)
        {
            _channelNamePrefix = channelNamePrefix;
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _handlerTimeout = handlerTimeout == default 
                ? TimeSpan.FromSeconds(60) :
                handlerTimeout;
        }

        public virtual Task SubscribeAsync(TKey channel, Func<TKey, TValue, CancellationToken, Task> handler, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return GetSubscriber().SubscribeAsync(
                GetRedisChannel(channel),
                (c, v) =>
                {
                    var parsedChannel = GetChannelFromString(c);
                    var message = _serializer.Deserialize(v);
                    Task.Run(async () =>
                    {
                        using var cts = new CancellationTokenSource(_handlerTimeout);
                        try
                        {
                            await handler(parsedChannel, message, cts.Token).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            // Avoid having unobserved failed tasks
                            Trace.TraceError(ex.ToString());
                        }
                    });
                });
        }

        public virtual Task UnsubscribeAsync(TKey channel, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return GetSubscriber().UnsubscribeAsync(GetRedisChannel(channel));
        }

        public virtual Task PublishAsync(TKey channel, TValue message, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return GetSubscriber().PublishAsync(GetRedisChannel(channel), _serializer.Serialize(message));
        }

        protected virtual string GetRedisChannel(TKey channel) => $"{_channelNamePrefix}:{KeyToString(channel)}";

        protected virtual TKey GetChannelFromString(string value)
        {
            // Trim the channel name prefix
            var keyWithoutPrefix = value.Substring(_channelNamePrefix.Length + 1, value.Length - _channelNamePrefix.Length - 1);
            return GetKeyFromString(keyWithoutPrefix);
        }
    }
}
