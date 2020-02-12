using System;
using Microsoft.EntityFrameworkCore;
using Take.Elephant.EntityFramework;

namespace Take.Elephant.Tests.EntityFramework
{
    public class EntityFrameworkGuidItemMapFacts : GuidItemMapFacts
    {
        public override IMap<Guid, Item> Create()
        {
            throw new NotImplementedException();
        }
    }
    
    public class TestContext : DbContext
    {
        public DbSet<Item> Type { get; set; }
        
    }
}