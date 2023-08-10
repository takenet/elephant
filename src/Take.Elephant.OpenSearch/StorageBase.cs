using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using OpenSearch.Client;
using Take.Elephant.OpenSearch.Mapping;

namespace Take.Elephant.OpenSearch
{
    public abstract class StorageBase<T> : IQueryableStorage<T> where T : class
    {
        private readonly ConnectionSettings _connectionSettings;
        private readonly ConcurrentDictionary<string, Func<T, object>> _propertiesDictionary;
        protected readonly IOpenSearchClient _openSearchClient;
        protected IMapping Mapping { get; }

        public StorageBase(string hostname, string username, string password, string defaultIndex, IMapping mapping)
        {
            _propertiesDictionary = new ConcurrentDictionary<string, Func<T, object>>();
            _connectionSettings = new ConnectionSettings(new Uri(hostname))
                .BasicAuthentication(username, password)
                .DefaultIndex(defaultIndex);
            Mapping = mapping;
            _openSearchClient = new OpenSearchClient(_connectionSettings);
        }

        public StorageBase(ConnectionSettings connectionSettings, IMapping mapping)
        {
            _propertiesDictionary = new ConcurrentDictionary<string, Func<T, object>>();
            _connectionSettings = connectionSettings;
            Mapping = mapping;
            _openSearchClient = new OpenSearchClient(_connectionSettings);
        }

        public StorageBase(IOpenSearchClient openClient, IMapping mapping)
        {
            _propertiesDictionary = new ConcurrentDictionary<string, Func<T, object>>();
            Mapping = mapping;
            _openSearchClient = openClient;
        }

        public async Task<QueryResult<T>> QueryAsync<TResult>(
            Expression<Func<T, bool>> where,
            Expression<Func<T, TResult>> select, int skip, int take,
            CancellationToken cancellationToken = default)
        {
            var queryDescriptor = where.ParseToQueryContainer<T>();

            var result = await _openSearchClient.SearchAsync<T>(s => s
                .Index(Mapping.Index)
                .Query(_ => queryDescriptor)
                .From(skip).Size(take), cancellationToken);

            return new QueryResult<T>(result.Documents, (int)result.Total);
        }

        public async Task<bool> ContainsKeyAsync(string key, CancellationToken cancellationToken = default)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var response = await _openSearchClient.DocumentExistsAsync<T>(key, d => d
                .Index(Mapping.Index),
                cancellationToken);

            return response.Exists;
        }

        public async Task<T> GetValueOrDefaultAsync(string key, CancellationToken cancellationToken = default)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var result = await _openSearchClient.GetAsync<T>(key,
                d => d
                .Index(Mapping.Index),
                cancellationToken);

            return result?.Source;
        }

        public async Task<bool> TryAddAsync(string key, T value, bool overwrite = false, CancellationToken cancellationToken = default)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            if (overwrite || !await ContainsKeyAsync(key, cancellationToken))
            {
                var result = await _openSearchClient.IndexAsync(new IndexRequest<T>(value,
                    Mapping.Index, key),
                    cancellationToken);

                return result.IsValid;
            }

            return false;
        }

        public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var result = await _openSearchClient.DeleteAsync<T>(key,
                d => d.Index(Mapping.Index),
                             cancellationToken);

            return result.IsValid;
        }

        protected string GetPropertyValue(T entity, string property)
        {
            if (property == null)
            {
                throw new ArgumentNullException(property);
            }

            var propertyAcessor = _propertiesDictionary
                .GetOrAdd(property, e =>
                    TypeUtil.BuildGetAccessor(
                        typeof(T).GetProperties()
                        .Single(p => p.Name == property)));

            return propertyAcessor(entity)?.ToString();
        }
    }
}