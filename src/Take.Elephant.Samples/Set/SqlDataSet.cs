using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Samples.Set
{
    public class SqlDataSet : SqlSet<Data>, IDataSet
    {
        private static readonly ITable table;

        static SqlDataSet()
        {
            table = TableBuilder
                .WithName("DataSet")
                .WithKeyColumnsFromTypeProperties<Data>()
                .Build();
        }

        public SqlDataSet() 
            : base(@"Server=(localdb)\MSSQLLocalDB;Database=Elephant;Integrated Security=true", table, new TypeMapper<Data>(table))
        {
        }

    }
}