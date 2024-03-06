﻿using AutoFixture;

namespace Take.Elephant.Tests
{
    public abstract class SortedSetMapFacts<TKey, TValue> : MapFacts<TKey, ISortedSet<TValue>>
    {
        public override void AssertEquals<T>(T actual, T expected)
        {
            if (typeof(ISortedSet<TValue>).IsAssignableFrom(typeof(T)) &&
                actual != null && expected != null)
            {
                var actualSet = (ISortedSet<TValue>)actual;
                var expectedSet = (ISortedSet<TValue>)expected;
                
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

        public override ISortedSet<TValue> CreateValue(TKey key)
        {
            return CreateValue(key, true);
        }

        public abstract ISortedSet<TValue> CreateValue(TKey key, bool populate);
    }
}