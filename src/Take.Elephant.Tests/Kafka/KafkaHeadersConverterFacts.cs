using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Take.Elephant.Kafka;
using Xunit;

namespace Take.Elephant.Tests.Kafka
{
    [Trait("Category", nameof(Kafka))]
    public class KafkaHeadersConverterFacts
    {
        [Fact]
        public void ToDictionary_WithNullHeaders_ShouldReturnEmptyReadOnlyDictionary()
        {
            var result = KafkaHeadersConverter.ToDictionary(null);

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact]
        public void ToDictionary_WithEmptyHeaders_ShouldReturnEmptyReadOnlyDictionary()
        {
            var result = KafkaHeadersConverter.ToDictionary(new Headers());

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact]
        public void ToDictionary_WithDuplicateKeys_ShouldKeepLastValue()
        {
            var headers = new Headers
            {
                new Header("x-origin", new byte[] { 1 }),
                new Header("x-origin", new byte[] { 2 })
            };

            var result = KafkaHeadersConverter.ToDictionary(headers);

            Assert.NotNull(result);
            Assert.Single(result);
            Assert.Equal(new byte[] { 2 }, result["x-origin"]);
        }

        [Fact]
        public void ToDictionary_ShouldCloneByteArrays()
        {
            var sourceBytes = new byte[] { 9, 8, 7 };
            var headers = new Headers
            {
                new Header("x-test", sourceBytes)
            };

            var result = KafkaHeadersConverter.ToDictionary(headers);

            Assert.NotNull(result);
            Assert.True(result.ContainsKey("x-test"));
            Assert.NotSame(sourceBytes, result["x-test"]);
            Assert.Equal(new byte[] { 9, 8, 7 }, result["x-test"]);

            sourceBytes[0] = 1;
            Assert.Equal(new byte[] { 9, 8, 7 }, result["x-test"]);
        }

        [Fact]
        public void ToDictionary_WithNonEmptyHeaders_ShouldReturnReadOnlyDictionary()
        {
            var headers = new Headers
            {
                new Header("x-test", new byte[] { 9, 8, 7 })
            };

            var result = KafkaHeadersConverter.ToDictionary(headers);
            var writableResult = Assert.IsAssignableFrom<IDictionary<string, byte[]>>(result);

            Assert.Throws<NotSupportedException>(() => writableResult["x-test"] = new byte[] { 1 });
            Assert.Equal(new byte[] { 9, 8, 7 }, result["x-test"]);
        }

        [Fact]
        public void BuildConsumedMessage_ShouldPreserveItemAndHeaders()
        {
            var item = new Payload { Value = "ok" };
            var headers = new Headers
            {
                new Header("x-origin", new byte[] { 3 })
            };

            var result = KafkaHeadersConverter.BuildConsumedMessage(item, headers);

            Assert.Same(item, result.Item);
            Assert.True(result.Headers.ContainsKey("x-origin"));
            Assert.Equal(new byte[] { 3 }, result.Headers["x-origin"]);
        }

        private sealed class Payload
        {
            public string Value { get; set; }
        }
    }
}
