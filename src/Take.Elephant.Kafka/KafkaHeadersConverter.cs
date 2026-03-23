using System.Collections.Generic;
using System.Collections.ObjectModel;
using Confluent.Kafka;

namespace Take.Elephant.Kafka
{
    internal static class KafkaHeadersConverter
    {
        internal static KafkaConsumedMessage<T> BuildConsumedMessage<T>(T item, Headers headers)
        {
            return new KafkaConsumedMessage<T>(item, ToDictionary(headers), headersAreSafe: true);
        }

        internal static IReadOnlyDictionary<string, byte[]> ToDictionary(Headers headers)
        {
            if (headers == null || headers.Count == 0)
            {
                return KafkaConsumedMessageDefaults.EmptyHeaders;
            }

            var result = new Dictionary<string, byte[]>(headers.Count);
            foreach (var header in headers)
            {
                if (header?.Key == null)
                {
                    continue;
                }

                var valueBytes = header.GetValueBytes();
                result[header.Key] = valueBytes != null ? (byte[])valueBytes.Clone() : null;
            }

            if (result.Count == 0)
            {
                return KafkaConsumedMessageDefaults.EmptyHeaders;
            }

            return new ReadOnlyDictionary<string, byte[]>(result);
        }
    }
}