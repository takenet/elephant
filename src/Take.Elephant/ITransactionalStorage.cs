using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a storage that support transaction operations.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface ITransactionalStorage<T>
    {
        /// <summary>
        /// Commits a storage transaction.
        /// </summary>
        /// <param name="transaction"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task CommitAsync(StorageTransaction<T> transaction, CancellationToken cancellationToken);
        
        /// <summary>
        /// Rollbacks the execution of a storage transaction.
        /// </summary>
        /// <param name="transaction"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task RollbackAsync(StorageTransaction<T> transaction, CancellationToken cancellationToken);
    }
    
    /// <summary>
    /// Represents a storage transaction.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class StorageTransaction<T>
    {
        public StorageTransaction(object transaction, T item)
        {
            Item = item;
            Transaction = transaction;
        }
                        
        /// <summary>
        /// The transaction object, specific for each transaction engine.
        /// </summary>
        public object Transaction { get; }
        
        /// <summary>
        /// The item related to the transaction.
        /// </summary>
        public T Item { get; }
    }
}