namespace Takenet.Elephant
{
    /// <summary>
    /// Represents a map that contains a set on unique items.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TItem"></typeparam>
    public interface ISetMap<TKey, TItem> : IMap<TKey, ISet<TItem>>
    {
    }
}
