using System.Collections.Generic;
using Confluent.Kafka;

namespace Take.Elephant.Kafka
{
    /// <summary>
    /// Provides Kafka message headers to be injected into every produced message.
    /// Implementations are resolved at produce-time, so headers can be dynamic
    /// (e.g. trace IDs) or static (e.g. deployment metadata read once at startup).
    /// </summary>
    public interface IKafkaHeaderProvider
    {
        IEnumerable<IHeader> GetHeaders();
    }
}
