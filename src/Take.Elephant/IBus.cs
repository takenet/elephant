using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a message bus with a pub/sub infrastructure.
    /// </summary>
    /// <typeparam name="TChannel"></typeparam>
    /// <typeparam name="TMessage"></typeparam>
    public interface IBus<TChannel, TMessage>
    {
        /// <summary>
        /// Subscribes a channel and register a handler for receiving the published messages.
        /// </summary>
        /// <param name="channel"></param>
        /// <param name="handler"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task SubscribeAsync(TChannel channel, Func<TChannel, TMessage, CancellationToken, Task> handler, CancellationToken cancellationToken);

        /// <summary>
        /// Unsubscribes a channel and stops receiving updates from a channel.
        /// </summary>
        /// <param name="channel"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task UnsubscribeAsync(TChannel channel, CancellationToken cancellationToken);

        /// <summary>
        /// Publishes a message to a channel. The message is delivered to all channel subscribers.
        /// </summary>
        /// <param name="channel"></param>
        /// <param name="message"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task PublishAsync(TChannel channel, TMessage message, CancellationToken cancellationToken);
    }
}
