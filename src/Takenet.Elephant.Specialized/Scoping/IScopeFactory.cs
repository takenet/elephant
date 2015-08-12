namespace Takenet.Elephant.Specialized.Scoping
{
    public interface IScopeFactory
    {
        IScope CreateScope(string name);
    }
}