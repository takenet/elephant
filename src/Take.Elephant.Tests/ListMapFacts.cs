using AutoFixture;

namespace Take.Elephant.Tests
{
    public abstract class ListMapFacts<TKey, TValue> : MapFacts<TKey, IPositionList<TValue>>
    {
        public override void AssertEquals<T>(T actual, T expected)
        {
            if (typeof(IList<TValue>).IsAssignableFrom(typeof(T)) &&
                actual != null && expected != null)
            {
                var actualSet = (IList<TValue>)actual;
                var expectedSet = (IList<TValue>)expected;
                
                base.AssertCollectionEquals(actualSet, expectedSet);
            }
            else
            {
                base.AssertEquals(actual, expected);
            }
        }

        public virtual TValue CreateItem()
        {
            return Fixture.Create<TValue>();
        }

        public override IPositionList<TValue> CreateValue(TKey key)
        {
            return CreateValue(key, true);
        }

        public abstract IPositionList<TValue> CreateValue(TKey key, bool populate);
    }
}