using System.Collections.Generic;
using NFluent;

namespace Takenet.SimplePersistence.Tests
{
    public class AssertionBase
    {
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
    }
}