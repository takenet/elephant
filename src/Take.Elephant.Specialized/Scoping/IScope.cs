using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Specialized.Scoping
{
    public interface IScope
    {
        string Name { get; }

        Task ClearAsync(CancellationToken cancellationToken);
    }
}
