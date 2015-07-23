using System;
using System.Threading.Tasks;
using Moq;
using Takenet.Elephant.Memory;
using Takenet.Elephant.Specialized.Replication;
using Xunit;

namespace Takenet.Elephant.Tests.Replication
{
    public abstract class ReplicationMapFacts<TKey, TValue> : MapFacts<TKey, TValue>
    {
        public override IMap<TKey, TValue> Create()
        {            
            return Create(new Map<TKey, TValue>(), new Map<TKey, TValue>());
        }

        public IMap<TKey, TValue> Create(IMap<TKey, TValue> master, IMap<TKey, TValue> slave)
        {
            return new ReplicationMap<TKey, TValue>(master, slave, TimeSpan.FromSeconds(30));
        }

        [Fact(DisplayName = "TryAddToDownMasterShouldAddToSlave")]
        public virtual async Task TryAddToDownMasterShouldAddToSlave()
        {
            // Arrange
            var master = new Mock<IMap<TKey, TValue>>();
            master
                .Setup(m => m.TryAddAsync(It.IsAny<TKey>(), It.IsAny<TValue>(), It.IsAny<bool>()))
                .ThrowsAsync(new Exception())
                .Verifiable();
            master
                .Setup(m => m.GetValueOrDefaultAsync(It.IsAny<TKey>()))
                .ThrowsAsync(new Exception());

            var slave = new Map<TKey, TValue>();
            var map = Create(master.Object, slave);
            var key = CreateKey();
            var value = CreateValue(key);

            // Act
            var actual = await map.TryAddAsync(key, value, false);

            // Assert
            AssertIsTrue(actual);
            AssertEquals(await map.GetValueOrDefaultAsync(key), value);            
            master.VerifyAll();
            AssertIsTrue(await slave.ContainsKeyAsync(key));
            AssertEquals(await slave.GetValueOrDefaultAsync(key), value);
        }

        [Fact(DisplayName = "TryAddToDownMasterMultipleTimesShouldSynchronizeAfterRecover")]
        public virtual async Task TryAddToDownMasterMultipleTimesShouldSynchronizeAfterRecover()
        {
            // Arrange
            var master = new Mock<IMap<TKey, TValue>>();
            var slave = new Map<TKey, TValue>();
            var map = Create(master.Object, slave);
            var key1 = CreateKey();
            var value1 = CreateValue(key1);
            var key2 = CreateKey();
            var value2 = CreateValue(key2);
            var key3 = CreateKey();
            var value3 = CreateValue(key3);
            master
                .SetupSequence(m => m.TryAddAsync(It.IsAny<TKey>(), It.IsAny<TValue>(), It.IsAny<bool>()))
                .Throws(new Exception())
                .Throws(new Exception())
                .Returns(Task.FromResult(true));                            
            master
                .Setup(m => m.GetValueOrDefaultAsync(It.IsAny<TKey>()))
                .ThrowsAsync(new Exception());
            master
                .Setup(m => m.TryAddAsync(key1, value1, false))
                .ReturnsAsync(true)
                .Verifiable();
            master
                .Setup(m => m.TryAddAsync(key2, value2, false))
                .ReturnsAsync(true)
                .Verifiable();
            master
                .Setup(m => m.TryAddAsync(key3, value3, false))
                .ReturnsAsync(true)
                .Verifiable();

            // Act
            var actual1 = await map.TryAddAsync(key1, value1, false);
            var actual2 = await map.TryAddAsync(key2, value2, false);
            var actual3 = await map.TryAddAsync(key3, value3, false);

            // Assert
            AssertIsTrue(actual1);
            AssertIsTrue(actual2);
            AssertIsTrue(actual3);
            AssertEquals(await map.GetValueOrDefaultAsync(key1), value1);
            AssertEquals(await map.GetValueOrDefaultAsync(key2), value2);
            master.VerifyAll();
        }
    }
}
