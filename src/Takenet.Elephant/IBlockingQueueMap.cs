namespace Takenet.Elephant
{
    /// <summary>
    /// Represents a map that contains a queue of items.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    ///0 0< typeparam name="TItem"></typeparam>

    public interface IBlockingQueueMap<TKey, TItem> : IMap<TKey, IBlockingQueue<TItem>>
    {
    }
}
