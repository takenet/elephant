using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
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
        public virtual async Task SubscribeSucceeds()
        {
            // Arrange
            var channel = CreateChannel();
            var target = Create();
 
            // Act
            await target.SubscribeAsync(channel, (_, __, ___) => Task.CompletedTask,  CancellationToken);
        }

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
        public virtual async Task PublishSucceeds()
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
        
        [Fact]
        public virtual async Task PublishWithMultipleSubscribersSucceeds()
        {
            // Arrange
            var channel = CreateChannel();
            var message = CreateMessage();
            var target = Create();
            var subscribersCount = 100;
            var subscribersTcsList = new List<TaskCompletionSource<(TChannel Channel, TMessage Message)>>(); 
            await using var _ = CancellationToken.Register(() => subscribersTcsList.ForEach(i => i.TrySetCanceled()));
            
            for (int i = 0; i < subscribersCount; i++)
            {
                var tcs = new TaskCompletionSource<(TChannel Channel, TMessage Message)>();
                subscribersTcsList.Add(tcs);
                await target.SubscribeAsync(channel, async (c, m, ct) => tcs.SetResult((c, m)), CancellationToken);
            }
            
            // Act
            await target.PublishAsync(channel, message, CancellationToken);
            await Task.WhenAll(subscribersTcsList.Select(v => v.Task));
            
            // Assert
            foreach (var tcs in subscribersTcsList)
            {
                var actual = tcs.Task.Result;
                actual.Channel.ShouldBe(channel);
                actual.Message.ShouldBe(message);
            }
        }
        
        [Fact]
        public virtual async Task PublishMultipleMessagesSucceeds()
        {
            // Arrange
            var messagesCount = 100;
            var channelMessageDictionary = Enumerable
                .Range(0, messagesCount)
                .Select(_ => (CreateChannel(), CreateMessage()))
                .ToDictionary(i => i.Item1, i => i.Item2);
            var target = Create();
            var messagesTcsList = new List<TaskCompletionSource<(TChannel Channel, TMessage Message)>>(); 
            await using var _ = CancellationToken.Register(() => messagesTcsList.ForEach(i => i.TrySetCanceled()));
            foreach (var channel in channelMessageDictionary.Keys)
            {
                var tcs = new TaskCompletionSource<(TChannel Channel, TMessage Message)>();
                messagesTcsList.Add(tcs);
                await target.SubscribeAsync(channel, async (c, m, ct) => tcs.SetResult((c, m)), CancellationToken);
            }
            
            // Act
            foreach (var (channel, message) in channelMessageDictionary)
            {
                await target.PublishAsync(channel, message, CancellationToken);
            }
            var actuals = await Task.WhenAll(messagesTcsList.Select(v => v.Task));

            // Assert
            var actualDictionary = actuals.ToDictionary(i => i.Channel, i => i.Message);
            actualDictionary.Keys.ShouldBe(channelMessageDictionary.Keys, true);
            actualDictionary.Values.ShouldBe(channelMessageDictionary.Values, true);
            
            foreach (var actual in actuals)
            {
                channelMessageDictionary.ShouldContainKeyAndValue(actual.Channel, actual.Message);
            }
        }
        
        [Fact]
        public virtual async Task UnsubscribeSucceeds()
        {
            // Arrange
            var channel = CreateChannel();
            var target = Create();
 
            // Act
            await target.UnsubscribeAsync(channel, CancellationToken);
        }

        [Fact]
        public virtual async Task UnsubscribeAfterSubscribedSucceeds()
        {
            // Arrange
            var channel = CreateChannel();
            var message = CreateMessage();
            var target = Create();
            var tcs = new TaskCompletionSource<(TChannel Channel, TMessage Message)>();
            await using var _ = CancellationToken.Register(() => tcs.TrySetCanceled());
            await target.SubscribeAsync(channel, async (c, m, ct) => tcs.SetResult((c, m)), CancellationToken);

            // Act
            await target.UnsubscribeAsync(channel, CancellationToken);
            await target.PublishAsync(channel, message, CancellationToken);
            await Task.Delay(500, CancellationToken);
            
            // Assert
            tcs.Task.Status.ShouldBe(TaskStatus.WaitingForActivation);
        }

        
        public void Dispose()
        {
            CancellationTokenSource.Dispose();
        }
    }
}