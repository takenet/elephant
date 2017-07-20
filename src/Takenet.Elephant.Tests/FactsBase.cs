using System;
using System.Threading.Tasks;
using NFluent;
using Ploeh.AutoFixture;
using Ploeh.AutoFixture.Kernel;
using Xunit;
using Shouldly;

namespace Takenet.Elephant.Tests
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
            actual.ShouldBe(expected);
        }

        public virtual void AssertIsTrue(bool actual)
        {
            actual.ShouldBeTrue();
        }

        public virtual void AssertIsFalse(bool actual)
        {
            actual.ShouldBe(false);
        }

        public virtual void AssertIsDefault<T>(T actual)
        {
            actual.ShouldBe(default(T));
        }

        public virtual void AssertIsNull<T>(T actual) where T : class
        {
            actual.ShouldBeNull();
        }

        public virtual void AssertIsNotNull<T>(T actual) where T : class
        {
            actual.ShouldNotBeNull();
        }

        public virtual async Task AssertThrowsAsync<TException>(Func<Task> func) where TException : Exception
        {
            try
            {
                await func().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                ex.ShouldBeAssignableTo<TException>();
            }
        }
    }
}