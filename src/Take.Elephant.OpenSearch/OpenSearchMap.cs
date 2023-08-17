using System.Threading;
using System.Threading.Tasks;
using OpenSearch.Client;
using Take.Elephant.OpenSearch.Mapping;

namespace Take.Elephant.OpenSearch
{
    public class OpenSearchMap<TKey, T> : StorageBase<T>, IMap<TKey, T> where T : class
    {
        public OpenSearchMap(string hostname, string username, string password, string defaultIndex, IMapping mapping)
            : base(hostname, username, password, defaultIndex, mapping)
        {
        }

        public OpenSearchMap(ConnectionSettings connectionSettings, IMapping mapping)
            : base(connectionSettings, mapping)
        {
        }

        public OpenSearchMap(IOpenSearchClient client, IMapping mapping)
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