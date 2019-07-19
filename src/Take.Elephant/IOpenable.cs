using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant
{
    /// <summary>
    /// Defines a service that can be opened.
    /// </summary>
    public interface IOpenable
    {
        /// <summary>
        /// Opens the service
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task OpenAsync(CancellationToken cancellationToken);
    }
}