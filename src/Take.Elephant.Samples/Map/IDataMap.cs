using System;

namespace Take.Elephant.Samples.Map
{
    /// <summary>
    /// A good idea to use the library is define interfaces with the storage capabilities that your application needs for each entity.
    /// With that information, its easier to choose the most appropriate storage engine in each case.
    /// In the example below, it is possible to use Memory, Redis and SQL engines, since the available implementations for these engines supports the inherited interfaces.
    /// </summary>
    public interface IDataMap : IMap<Guid, Data>, IPropertyMap<Guid, Data>
    {

    }
}
