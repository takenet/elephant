using System;
using System.Runtime.Serialization;

namespace Take.Elephant.Specialized.Cache
{
    [DataContract]
    public class SynchronizationEvent<TKey>
    {
        [DataMember]
        public TKey Key { get; set; }

        [DataMember]
        public Guid Instance { get; set; }
    }
}