using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.ElasticSearch;
using Take.Elephant.ElasticSearch.Mapping;

namespace Take.Elephant.Elasticsearch
{
    public class StorageBase<T> : IQueryableStorage<T> where T : class
    {
        protected readonly ElasticClient ElasticClient;
        private readonly IElasticsearchConfiguration _configuration;

        protected IMapping Mapping { get; }

        private string _index => Mapping.Index ?? _configuration.DefaultIndex;

        public StorageBase(IElasticsearchConfiguration configuration, IMapping mapping)
        {
            _configuration = configuration;

            var settings = new ConnectionSettings(new Uri(_configuration.Hostname))
                .BasicAuthentication(_configuration.Username, _configuration.Password)
                .DefaultIndex(_configuration.DefaultIndex);

            Mapping = mapping;
            ElasticClient = new ElasticClient(settings);
        }

        public StorageBase(ElasticClient elasticClient)
        {
            ElasticClient = elasticClient;
        }

        public async Task<QueryResult<T>> QueryAsync<TResult>(
            Expression<Func<T, bool>> where, 
            Expression<Func<T, TResult>> select, int skip, int take, 
            CancellationToken cancellationToken = default)
        {
            var queryDescriptor = where.ParseToQueryContainer<T>();

            var result = await ElasticClient.SearchAsync<T>(s => s
                .Index(_index)
                .Query(_ => queryDescriptor)
                .From(skip).Size(take), cancellationToken);

            return new QueryResult<T>(new AsyncEnumerableWrapper<T>(result.Documents), (int)result.Total);
        }

        public async Task<bool> ContainsKeyAsync(string key, CancellationToken cancellationToken = default)
        {
            var response = await ElasticClient.DocumentExistsAsync<T>(key, d => d
                .Index(_index), cancellationToken);

            return response.Exists;
        }

        public async Task<T> GetValueOrDefaultAsync(string key, CancellationToken cancellationToken = default)
        {
            var result = await ElasticClient.SearchAsync<T>(s => s
                .Index(_index)
                .Query(q => q.Ids(a => a.Values(key.ToString()))), cancellationToken);

            return result.Documents.FirstOrDefault();
        }

        public async Task<bool> TryAddAsync(string key, T value, bool overwrite = false, CancellationToken cancellationToken = default)
        {
            if (overwrite || !await ContainsKeyAsync(key, cancellationToken))
            {
                var result = await ElasticClient.IndexAsync(new IndexRequest<T>(value, _index,
                    key.ToString()), cancellationToken);

                return result.IsValid;
            }

            return false;
        }

        public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
        {
            var result = await ElasticClient.DeleteAsync<T>(key, 
                d => d.Index(_index), cancellationToken);

            return result.IsValid;
        }

        protected string GetPropertyValue(T entity, string property)
        {
            return entity.GetType().GetProperties()
               .Single(p => p.Name == property)
               .GetValue(entity, null)
               .ToString();
        }
    }
}
