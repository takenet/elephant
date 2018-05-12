using System;
using System.Threading.Tasks;
using Moq;
using Take.Elephant.Memory;
using Take.Elephant.Specialized;
using Xunit;

namespace Take.Elephant.Tests.Specialized
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

        [Fact(DisplayName = "TryAddWhenMasterIsUpShouldAddToSlave")]
        public virtual async Task TryAddWhenMasterIsUpShouldAddToSlave()
        {
            // Arrange
            var master = new Map<TKey, TValue>();
            var slave = new Map<TKey, TValue>();
            var map = Create(master, slave);
            var key = CreateKey();
            var value = CreateValue(key);

            // Act
            var actual = await map.TryAddAsync(key, value, false);

            // Assert
            AssertIsTrue(actual);
            AssertEquals(await map.GetValueOrDefaultAsync(key), value);
            AssertIsTrue(await master.ContainsKeyAsync(key));
            AssertEquals(await master.GetValueOrDefaultAsync(key), value);
            AssertIsTrue(await slave.ContainsKeyAsync(key));
            AssertEquals(await slave.GetValueOrDefaultAsync(key), value);
        }

        [Fact(DisplayName = "TryAddWhenMasterIsDownShouldAddToSlave")]
        public virtual async Task TryAddWhenMasterIsDownShouldAddToSlave()
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

        [Fact(DisplayName = "TryAddMultipleTimesWhenMasterDownShouldSynchronizeAfterRecovery")]
        public virtual async Task TryAddMultipleTimesWhenMasterDownShouldSynchronizeAfterRecovery()
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

            // Act
            var actual1 = await map.TryAddAsync(key1, value1, true);
            var actual2 = await map.TryAddAsync(key2, value2, true);
            var actual3 = await map.TryAddAsync(key3, value3, true);

            // Assert
            AssertIsTrue(actual1);
            AssertIsTrue(actual2);
            AssertIsTrue(actual3);
            AssertEquals(await map.GetValueOrDefaultAsync(key1), value1);
            AssertEquals(await map.GetValueOrDefaultAsync(key2), value2);
            master.Verify(m => m.TryAddAsync(key1, value1, true), Times.Once);
            master.Verify(m => m.TryAddAsync(key1, value1, true), Times.Once);
            master.Verify(m => m.TryAddAsync(key1, value1, true), Times.Once);
            // Synchronization
            master.Verify(m => m.TryAddAsync(key1, value1, false), Times.Once);
            master.Verify(m => m.TryAddAsync(key2, value2, false), Times.Once);
            master.Verify(m => m.TryAddAsync(key3, value3, false), Times.Once);
        }


        [Fact(DisplayName = "TryAddMultipleTimesWhenMasterIsUpThenDownShouldSynchronizeAfterRecovery")]
        public virtual async Task TryAddMultipleTimesWhenMasterIsUpThenDownShouldSynchronizeAfterRecovery()
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
                    .Returns(Task.FromResult(true))
                    .Throws(new Exception())
                    .Returns(Task.FromResult(true));

            master
                .Setup(m => m.GetValueOrDefaultAsync(It.IsAny<TKey>()))
                .ThrowsAsync(new Exception());

            // Act
            var actual1 = await map.TryAddAsync(key1, value1, true);
            var actual2 = await map.TryAddAsync(key2, value2, true);
            var actual3 = await map.TryAddAsync(key3, value3, true);

            // Assert
            AssertIsTrue(actual1);
            AssertIsTrue(actual2);
            AssertIsTrue(actual3);
            AssertEquals(await map.GetValueOrDefaultAsync(key1), value1);
            AssertEquals(await map.GetValueOrDefaultAsync(key2), value2);
            AssertEquals(await map.GetValueOrDefaultAsync(key3), value3);
            master.Verify(m => m.TryAddAsync(key1, value1, true), Times.Once);
            master.Verify(m => m.TryAddAsync(key1, value1, true), Times.Once);
            master.Verify(m => m.TryAddAsync(key1, value1, true), Times.Once);
            // Synchronization
            master.Verify(m => m.TryAddAsync(key1, value1, false), Times.Once);
            master.Verify(m => m.TryAddAsync(key2, value2, false), Times.Once);
            master.Verify(m => m.TryAddAsync(key3, value3, false), Times.Once);
        }

        [Fact(DisplayName = "TryAddMultipleTimesWhenMasterIsUpShouldNotSynchronize")]
        public virtual async Task TryAddMultipleTimesWhenMasterIsUpShouldNotSynchronize()
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
                    .Returns(Task.FromResult(true))
                    .Returns(Task.FromResult(true))
                    .Returns(Task.FromResult(true));

            master
                .Setup(m => m.GetValueOrDefaultAsync(It.IsAny<TKey>()))
                .ThrowsAsync(new Exception());

            // Act
            var actual1 = await map.TryAddAsync(key1, value1, true);
            var actual2 = await map.TryAddAsync(key2, value2, true);
            var actual3 = await map.TryAddAsync(key3, value3, true);

            // Assert
            AssertIsTrue(actual1);
            AssertIsTrue(actual2);
            AssertIsTrue(actual3);
            AssertEquals(await map.GetValueOrDefaultAsync(key1), value1);
            AssertEquals(await map.GetValueOrDefaultAsync(key2), value2);
            AssertEquals(await map.GetValueOrDefaultAsync(key3), value3);
            master.Verify(m => m.TryAddAsync(key1, value1, true), Times.Once);
            master.Verify(m => m.TryAddAsync(key1, value1, true), Times.Once);
            master.Verify(m => m.TryAddAsync(key1, value1, true), Times.Once);
            // Synchronization
            master.Verify(m => m.TryAddAsync(key1, value1, false), Times.Never);
            master.Verify(m => m.TryAddAsync(key2, value2, false), Times.Never);
            master.Verify(m => m.TryAddAsync(key3, value3, false), Times.Never);
        }
    }
}
