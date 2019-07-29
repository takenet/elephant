using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.ElasticSearch.Mapping;

namespace Take.Elephant.ElasticSearch
{
    public class ElasticSearchSet<T> : StorageBase<T>, ISet<T> where T : class
    {
        protected string KeyProperty;

        public ElasticSearchSet(string keyProperty, IElasticSearchConfiguration configuration, IMapping mapping)
            : base(configuration, mapping)
        {
        }

        public ElasticSearchSet(IElasticClient client, IMapping mapping)
            : base(client, mapping)
        {
        }

        public async Task AddAsync(T value, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            var documentId = GetKeyValue(value);
            await TryAddAsync(documentId, value, true, cancellationToken);
        }

        public async Task<IAsyncEnumerable<T>> AsEnumerableAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            var results = await ElasticClient.SearchAsync<T>(c => c
            .Index(Mapping.Index)
            .Query(q => q.MatchAll()));

            return new AsyncEnumerableWrapper<T>(results.Documents);
        }

        public async Task<bool> ContainsAsync(T value, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            var documentId = GetKeyValue(value);
            return await ContainsKeyAsync(documentId, cancellationToken);
        }

        public async Task<long> GetLengthAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            var result = await ElasticClient.CountAsync<T>(c => c
                .Index(Mapping.Index)
                .Query(q => q.MatchAll()));

            return result.Count;
        }

        public async Task<bool> TryRemoveAsync(T value, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            var documentId = GetKeyValue(value);
            return await DeleteAsync(documentId, cancellationToken);
        }

        private string GetKeyValue(T entity) => GetPropertyValue(entity, Mapping.KeyField);
    }
}
