using System.Collections.Generic;
using NFluent;
using Ploeh.AutoFixture;

namespace Takenet.SimplePersistence.Tests
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
    }
}