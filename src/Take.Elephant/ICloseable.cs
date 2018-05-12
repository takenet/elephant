using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a service that can be closed.
    /// </summary>
    public interface ICloseable
    {
        /// <summary>
        /// Closes the service.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task CloseAsync(CancellationToken cancellationToken);
    }
}
