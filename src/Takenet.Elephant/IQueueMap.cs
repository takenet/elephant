namespace Takenet.Elephant
{
    /// <summary>
    /// Represents a map that contains a queue of items.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    ///0 0< typeparam name="TItem"></typeparam>

    public interface IQueueMap<TKey, TItem> : IMap<TKey, IQueue<TItem>>
    {
    }
}
