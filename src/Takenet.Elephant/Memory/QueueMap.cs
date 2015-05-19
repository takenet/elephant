namespace Takenet.Elephant.Memory
{
    /// <summary>
    /// Implements the <see cref="IQueueMap{TKey,TItem}"/> interface using the <see cref="Map{TKey,TValue}"/> and <see cref="Queue{T}"/> classes.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public class QueueMap<TKey, TValue> : Map<TKey, IQueue<TValue>>, IQueueMap<TKey, TValue>
    {
        public QueueMap()
            : base(() => new Queue<TValue>())
        {

        }
    }
}