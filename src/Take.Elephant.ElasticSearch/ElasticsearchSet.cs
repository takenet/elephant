using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Elasticsearch.Mapping;

namespace Take.Elephant.Elasticsearch
{
    public class ElasticsearchSet<T> : StorageBase<T>, ISet<T> where T : class
    {
        public ElasticsearchSet(string hostname, string username, string password, string defaultIndex, IMapping mapping)
            : base(hostname, username, password, defaultIndex, mapping)
        {
        }

        public ElasticsearchSet(ConnectionSettings connectionSettings, IMapping mapping)
            : base(connectionSettings, mapping)
        {
        }

        public ElasticsearchSet(IElasticClient client, IMapping mapping)
            : base(client, mapping)
        {
        }

        public Task AddAsync(T value, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            var documentId = GetKeyValue(value);
            return TryAddAsync(documentId, value, true, cancellationToken);
        }

        public async Task<IAsyncEnumerable<T>> AsEnumerableAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            var results = await ElasticClient.SearchAsync<T>(c => c
            .Index(Mapping.Index)
            .Type(Mapping.Type)
            .Query(q => q.MatchAll()));

            return new AsyncEnumerableWrapper<T>(results.Documents);
        }

        public Task<bool> ContainsAsync(T value, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            var documentId = GetKeyValue(value);
            return ContainsKeyAsync(documentId, cancellationToken);
        }

        public async Task<long> GetLengthAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            var result = await ElasticClient.CountAsync<T>(c => c
                .Index(Mapping.Index)
                .Type(Mapping.Type)
                .Query(q => q.MatchAll()));

            return result.Count;
        }

        public Task<bool> TryRemoveAsync(T value, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            var documentId = GetKeyValue(value);
            return DeleteAsync(documentId, cancellationToken);
        }

        private string GetKeyValue(T entity) => GetPropertyValue(entity, Mapping.KeyField);
    }
}
