using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Nest;
using Take.Elephant.Elasticsearch.Mapping;

namespace Take.Elephant.Elasticsearch
{
    public class ElasticsearchMap<TKey, T> : StorageBase<T>, IMap<TKey, T> where T : class
    {
        public ElasticsearchMap(string hostname, string username, string password, string defaultIndex, IMapping mapping)
            : base(hostname, username, password, defaultIndex, mapping)
        {
        }

        public ElasticsearchMap(ConnectionSettings connectionSettings, IMapping mapping)
            : base(connectionSettings, mapping)
        {
        }

        public ElasticsearchMap(IElasticClient client, IMapping mapping)
            : base(client, mapping)
        {
        }

        public Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default(CancellationToken))
        {
            return ContainsKeyAsync(key.ToString(), cancellationToken);
        }

        public Task<T> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default(CancellationToken))
        {
            return GetValueOrDefaultAsync(key.ToString(), cancellationToken);
        }

        public Task<bool> TryAddAsync(TKey key, T value, bool overwrite = false, CancellationToken cancellationToken = default(CancellationToken))
        {
            return TryAddAsync(key.ToString(), value, overwrite, cancellationToken);
        }

        public Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default(CancellationToken))
        {
            return DeleteAsync(key.ToString(), cancellationToken);
        }
    }
}
