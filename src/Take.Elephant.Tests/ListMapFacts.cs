using Ploeh.AutoFixture;

namespace Take.Elephant.Tests
{
    public abstract class ListMapFacts<TKey, TValue> : MapFacts<TKey, IList<TValue>>
    {
        public virtual TValue CreateItem()
        {
            return Fixture.Create<TValue>();
        }

        public override IList<TValue> CreateValue(TKey key)
        {
            return CreateValue(key, true);
        }

        public abstract IList<TValue> CreateValue(TKey key, bool populate);
    }
}
