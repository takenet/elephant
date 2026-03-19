using System.Collections.Generic;
using Confluent.Kafka;

namespace Take.Elephant.Kafka
{
    internal static class KafkaHeadersConverter
    {
        internal static KafkaConsumedMessage<T> BuildConsumedMessage<T>(T item, Headers headers)
        {
            return new KafkaConsumedMessage<T>(item, ToDictionary(headers));
        }

        internal static Dictionary<string, byte[]> ToDictionary(Headers headers)
        {
            if (headers == null || headers.Count == 0)
            {
                return null;
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

            return result;
        }
    }
}