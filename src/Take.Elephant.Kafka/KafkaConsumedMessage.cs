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
    public sealed class KafkaConsumedMessage<T>(T item, IReadOnlyDictionary<string, byte[]> headers)
    {

        private readonly IReadOnlyDictionary<string, byte[]> _headers =
             headers is null
                 ? KafkaConsumedMessageDefaults.EmptyHeaders
                 : new ReadOnlyDictionary<string, byte[]>(new Dictionary<string, byte[]>(headers));

        public T Item { get; } = item;

        public IReadOnlyDictionary<string, byte[]> Headers => _headers;

        public bool TryGetHeader(string key, out byte[] value)
        {
            if (!string.IsNullOrWhiteSpace(key)) return Headers.TryGetValue(key, out value);
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
    }
}
