using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Moq;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Takenet.Elephant.Memory;
using Takenet.Elephant.Specialized;
using Xunit;

namespace Takenet.Elephant.Tests.Specialized
{
    public class ReplicationMapFacts : GuidItemMapFacts
    {
        public override IMap<Guid, Item> Create()
        {
            var master = new Map<Guid, Item>();
            var slave = new Map<Guid, Item>();
            return Create(master, slave);
        }

        public IMap<Guid, Item> Create(IMap<Guid, Item> master, IMap<Guid, Item> slave)
        {
            return new ReplicationMap<Guid, Item>(master, slave, TimeSpan.MaxValue);
        }


        [Fact(DisplayName = "TryAddToDownMasterShouldAddToSlave")]
        public virtual async Task TryAddToDownMasterShouldAddToSlave()
        {
            // Arrange
            var master = new Mock<IMap<Guid, Item>>();
            master
                .Setup(m => m.TryAddAsync(It.IsAny<Guid>(), It.IsAny<Item>(), It.IsAny<bool>()))
                .ThrowsAsync(new Exception())
                .Verifiable();
            var slave = new Map<Guid, Item>();
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

        }

    }
}
