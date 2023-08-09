using System;
using System.Collections.Generic;
using System.Text;
using OpenSearch.Client;
using System.Threading.Tasks;
using System.Threading;
using Take.Elephant.OpenSearch.Mapping;

namespace Take.Elephant.OpenSearch
{
    public class OpenSearchSet<T> : StorageBase<T>, ISet<T> where T : class
    {
        public OpenSearchSet(string hostname, string username, string password, string defaultIndex, IMapping mapping)
            : base(hostname, username, password, defaultIndex, mapping)
        {
        }

        public OpenSearchSet(ConnectionSettings connectionSettings, IMapping mapping)
            : base(connectionSettings, mapping)
        {
        }

        public OpenSearchSet(IOpenSearchClient client, IMapping mapping)
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

        public async IAsyncEnumerable<T> AsEnumerableAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            var results = await _openSearchClient.SearchAsync<T>(c => c
                .Index(Mapping.Index)
                .Query(q => q.MatchAll()));

            foreach (var document in results.Documents)
            {
                yield return document;
            }
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
            var result = await _openSearchClient.CountAsync<T>(c => c
                .Index(Mapping.Index)
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