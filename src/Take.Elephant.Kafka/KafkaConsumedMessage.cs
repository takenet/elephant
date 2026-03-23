using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;

namespace Take.Elephant.Kafka
{
    internal static class KafkaConsumedMessageDefaults
    {
        internal static readonly IReadOnlyDictionary<string, byte[]> EmptyHeaders =
            new ReadOnlyDictionary<string, byte[]>(new Dictionary<string, byte[]>());
    }

    /// <summary>
    /// Represents a consumed Kafka payload with its message headers.
    /// </summary>
    /// <typeparam name="T">Payload type.</typeparam>
    public sealed class KafkaConsumedMessage<T>
    {
        public KafkaConsumedMessage(T item, IReadOnlyDictionary<string, byte[]> headers)
            : this(item, headers, headersAreSafe: false)
        {
        }

        internal KafkaConsumedMessage(T item, IReadOnlyDictionary<string, byte[]> headers, bool headersAreSafe)
        {
            Item = item;
            _headers = headersAreSafe
                ? UseProvidedHeaders(headers)
                : CreateReadOnlyHeaders(headers);
        }

        private readonly IReadOnlyDictionary<string, byte[]> _headers =
            KafkaConsumedMessageDefaults.EmptyHeaders;

        public T Item { get; }

        public IReadOnlyDictionary<string, byte[]> Headers => _headers;

        public bool TryGetHeader(string key, out byte[] value)
        {
            if (!string.IsNullOrWhiteSpace(key) && Headers.TryGetValue(key, out var headerValue))
            {
                value = headerValue != null ? (byte[])headerValue.Clone() : null;
                return true;
            }

            value = null;
            return false;

        }

        public bool TryGetHeaderAsUtf8String(string key, out string value)
        {
            value = null;
            if (!TryGetHeader(key, out var bytes) || bytes == null)
            {
                return false;
            }

            value = Encoding.UTF8.GetString(bytes);
            return true;
        }

        private static IReadOnlyDictionary<string, byte[]> CreateReadOnlyHeaders(
            IReadOnlyDictionary<string, byte[]> headers)
        {
            if (headers is null || headers.Count == 0)
            {
                return KafkaConsumedMessageDefaults.EmptyHeaders;
            }

            var result = new Dictionary<string, byte[]>(headers.Count);
            foreach (var header in headers)
            {
                result[header.Key] = header.Value != null ? (byte[])header.Value.Clone() : null;
            }

            return new ReadOnlyDictionary<string, byte[]>(result);
        }

        private static IReadOnlyDictionary<string, byte[]> UseProvidedHeaders(
            IReadOnlyDictionary<string, byte[]> headers)
        {
            if (headers is null || headers.Count == 0)
            {
                return KafkaConsumedMessageDefaults.EmptyHeaders;
            }

            return headers;
        }
    }
}
