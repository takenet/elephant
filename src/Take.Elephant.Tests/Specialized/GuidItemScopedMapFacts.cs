using System;
using Take.Elephant.Redis.Serializers;

namespace Take.Elephant.Tests.Specialized
{
    public abstract class GuidItemScopedMapFacts : ScopedMapFacts<Guid, Item>
    {
    }
}