using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized
{
    public interface ISemaphore
    {
        Task WaitAsync(CancellationToken cancellationToken);

        void Release();
    }
}
