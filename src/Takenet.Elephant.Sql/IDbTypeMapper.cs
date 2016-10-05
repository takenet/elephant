using System;
using System.Data;

namespace Takenet.Elephant.Sql
{
    /// <summary>
    /// Defines a CLR to Database type mapping service.
    /// </summary>
    public interface IDbTypeMapper
    {
        /// <summary>
        /// Maps a CLR type to a DbType of specified type.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        object ToDbType(object value, DbType type);

        /// <summary>
        /// Maps a DbType to a specified CLR type.
        /// </summary>
        /// <param name="dbValue">The database value.</param>
        /// <param name="type">The type.</param>
        /// <param name="propertyType">Type of the property.</param>
        /// <returns></returns>
        object FromDbType(object dbValue, DbType type, Type propertyType);
    }
}