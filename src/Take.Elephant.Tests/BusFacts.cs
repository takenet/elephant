using System;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using Shouldly;
using Xunit;

namespace Take.Elephant.Tests
{
    public abstract class BusFacts<TChannel, TMessage> : FactsBase, IDisposable
    {
        public BusFacts()
        {
            CancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        }
        
        public abstract IBus<TChannel, TMessage> Create();

        protected virtual TChannel CreateChannel() => Fixture.Create<TChannel>();
        
        protected virtual TMessage CreateMessage() => Fixture.Create<TMessage>();
        
        public CancellationTokenSource CancellationTokenSource { get; }

        public CancellationToken CancellationToken => CancellationTokenSource.Token;

        [Fact]
        public virtual async Task PublishWithNoSubscriberSucceeds()
        {
            // Arrange
            var channel = CreateChannel();
            var message = CreateMessage();
            var target = Create();
            
            // Act
            await target.PublishAsync(channel, message, CancellationToken);
        }
        
        [Fact]
        public virtual async Task PublishOneSubscriberSucceeds()
        {
            // Arrange
            var channel = CreateChannel();
            var message = CreateMessage();
            var target = Create();
            var tcs = new TaskCompletionSource<(TChannel Channel, TMessage Message)>();
            await using var _ = CancellationToken.Register(() => tcs.TrySetCanceled());
            await target.SubscribeAsync(channel, async (c, m, ct) => tcs.SetResult((c, m)), CancellationToken);
            
            // Act
            await target.PublishAsync(channel, message, CancellationToken);
            var actual = await tcs.Task;
            
            // Assert
            actual.Channel.ShouldBe(channel);
            actual.Message.ShouldBe(message);
        }

        public void Dispose()
        {
            CancellationTokenSource.Dispose();
        }
    }
}