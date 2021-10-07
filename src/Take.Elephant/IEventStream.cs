using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a stream storage container.
    /// </summary>
    /// <typeparam name="TKey">Defines a key to ensure that messages are consumed in the correct order</typeparam>
    /// <typeparam name="TEvent">Defines the object that will be sent in the stream</typeparam>
    public interface IEventStream<TKey, TEvent> : IEventStreamPublisher<TKey, TEvent>, IEventStreamConsumer<TKey, TEvent>
    {
        
    }

    public interface IEventStreamPublisher<TKey, TEvent>
    {
        /// <summary>
        /// Publish an item in the stream.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="item"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task PublishAsync(TKey key, TEvent item, CancellationToken cancellationToken);
    }

    public interface IEventStreamConsumer<TKey, TEvent>
    {
        /// <summary>
        /// Consume an item of the stream.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<(TKey key, TEvent item)> ConsumeOrDefaultAsync(CancellationToken cancellationToken);
    }
}
