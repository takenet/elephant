using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.ElasticSearch;
using Take.Elephant.ElasticSearch.Mapping;

namespace Take.Elephant.Elasticsearch
{
    public class ElasticSearchSet<T> : StorageBase<T>, ISet<T> where T : class
    {
        protected string KeyProperty;
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="keyProperty">Indicates the key column that will be used for elasticsearch (id)</param>
        /// <param name="host">Elasticsearch host</param>
        /// <param name="username">Elasticsearch username</param>
        /// <param name="password">Elasticsearch password</param>
        /// <param name="defaultIndex"></param>
        public ElasticSearchSet(string keyProperty, IElasticsearchConfiguration configuration, IMapping mapping) : base(configuration, mapping)
        {
        }

        public async Task AddAsync(T value, CancellationToken cancellationToken = default(CancellationToken))
        {
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
            var documentId = GetKeyValue(value);
            return await DeleteAsync(documentId, cancellationToken);
        }

        private string GetKeyValue(T entity) => GetPropertyValue(entity, Mapping.KeyField);
    }
}
