using Dawn;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Azure
{
    /// <summary>
    /// TODO: Internal while is not working.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class AzureTableList<T> : IList<T>
    {
        private readonly CloudTableClient _client;
        private readonly string _tableName;

        public AzureTableList(string storageConnectionString, string tableName)
        {
            Guard.Argument(storageConnectionString).NotNull().NotEmpty();
            Guard.Argument(tableName).NotNull().NotEmpty();
            var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            _client = storageAccount.CreateCloudTableClient();
            _tableName = tableName;
        }

        public Task AddAsync(T value)
        {
            var table = _client.GetTableReference(_tableName);
            table.CreateIfNotExistsAsync();

            throw new NotImplementedException();
        }

        public Task<IAsyncEnumerable<T>> AsEnumerableAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<long> RemoveAllAsync(T value)
        {
            throw new NotImplementedException();
        }
    }
}
