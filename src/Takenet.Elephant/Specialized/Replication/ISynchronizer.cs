using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized.Replication
{
    public interface ISynchronizer<in T>
    {
        Task SynchronizeAsync(T master, T slave);
    }
}