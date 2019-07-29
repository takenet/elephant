using Nest;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Elasticsearch
{
    public class SubKeySet<T> : StorageBase<T>, ISet<T> where T : class
    {
        private readonly string KeyValue;
        private readonly string KeyProperty;
        private readonly string SubKeyProperty;

        public SubKeySet(ElasticClient elasticClient, string keyProperty, string keyValue, string subKeyProperty) : base(elasticClient)
        {
            KeyProperty = keyProperty;
            KeyValue = keyValue;
            SubKeyProperty = subKeyProperty;
        }

        public async Task AddAsync(T value, CancellationToken cancellationToken = default(CancellationToken))
        {
            var documentId = $"{GetKeyValue(value)}:{GetKeyValue(value)}";
            await TryAddAsync(documentId, value, true, cancellationToken);
        }

        public Task<IAsyncEnumerable<T>> AsEnumerableAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        public Task<bool> ContainsAsync(T value, CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        public Task<long> GetLengthAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        public Task<bool> TryRemoveAsync(T value, CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        private string GetKeyValue(T entity) => GetPropertyValue(entity, KeyProperty);

        private string GetSubKeyValue(T entity) => GetPropertyValue(entity, SubKeyProperty);
    }
}
