using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    public interface ISynchronizer<in T>
    {
        Task SynchronizeAsync(T first, T second);
    }
}