using Nest;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.ElasticSearch;

namespace Take.Elephant.Elasticsearch
{
    public class ElasticsearchSetMap<TKey, T> : StorageBase<T>, ISetMap<TKey, T> where T : class
    {
        private readonly string KeyProperty;
        private readonly string SubKeyProperty;

        public ElasticsearchSetMap(string key, string subkey, IElasticsearchConfiguration configuration) : base(configuration)
        {
            KeyProperty = key;
            SubKeyProperty = subkey;
        }

        public Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        public async Task<ISet<T>> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default(CancellationToken)) =>
            new SubKeySet<T>(ElasticClient, KeyProperty, key.ToString(), SubKeyProperty);

        public async Task<ISet<T>> GetValueOrEmptyAsync(TKey key, CancellationToken cancellationToken = default(CancellationToken)) =>
            new SubKeySet<T>(ElasticClient, KeyProperty, key.ToString(), SubKeyProperty);

        public async Task<bool> TryAddAsync(TKey key, ISet<T> value, bool overwrite = false, CancellationToken cancellationToken = default(CancellationToken))
        {
            var items = await value.AsEnumerableAsync(cancellationToken).ConfigureAwait(false);
            var documents = new Dictionary<string, T>();
            await items.ForEachAsync(
                            async item =>
                            {
                                documents.Add($"{key}:{GetSubKeyValue(item)}", item);
                            },
                            cancellationToken);

            var descriptor = new BulkDescriptor();

            foreach (var document in documents)
            {
                descriptor.Index<T>(op => op
                    .Id(document.Key)
                    .Document(document.Value));
            }

            var result = await ElasticClient.BulkAsync(descriptor, cancellationToken);

            return result.IsValid;
        }

        public Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default(CancellationToken))
        {



            throw new NotImplementedException();
        }

        private string GetKeyValue(T entity) => GetPropertyValue(entity, KeyProperty);

        private string GetSubKeyValue(T entity) => GetPropertyValue(entity, SubKeyProperty);
    }
}
