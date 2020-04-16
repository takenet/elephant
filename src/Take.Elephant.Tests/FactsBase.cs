using System;
using System.Linq;
using System.Threading.Tasks;
using AutoFixture;
using NFluent;

namespace Take.Elephant.Tests
{
    public class FactsBase
    {
        public FactsBase()
        {
            Fixture = new Fixture();
        }

        protected readonly Fixture Fixture;

        public virtual void AssertEquals<T>(T actual, T expected)
        {
            Check.That(actual).IsEqualTo(expected);
        }

        public virtual void AssertIsTrue(bool actual)
        {
            Check.That(actual).IsTrue();
        }

        public virtual void AssertIsFalse(bool actual)
        {
            Check.That(actual).IsFalse();
        }

        public virtual void AssertIsDefault<T>(T actual)
        {
            Check.That(actual).Equals(default(T));
        }

        public virtual void AssertIsNull<T>(T actual) where T : class
        {
            Check.That(actual).IsNull();
        }

        public virtual void AssertIsNotNull<T>(T actual) where T : class
        {
            Check.That(actual).IsNotNull();
        }

        public virtual Task AssertThrowsAsync<TException>(Func<Task> func) where TException : Exception
        {
            var exec = Check.ThatAsyncCode(func).Throws<TException>();
            return TaskUtil.CompletedTask;
        }

        public virtual void AssertCollectionEquals<T>(ICollection<T> actual, ICollection<T> expected)
        {
            AssertCollectionEqualsAsync(actual, expected).GetAwaiter().GetResult();
        }
        
        public virtual async Task AssertCollectionEqualsAsync<T>(ICollection<T> actual, ICollection<T> expected)
        {
            var actualArray = await actual.AsEnumerableAsync().ToArrayAsync();
            var expectedArray = await expected.AsEnumerableAsync().ToArrayAsync();

            Check.That(actualArray).HasSize(expectedArray.Length);
            Check.That(actualArray).Contains(expectedArray);
        }
    }
}