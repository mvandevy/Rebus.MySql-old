using System;
using Rebus.MySql;
using MySql.Data.MySqlClient;

namespace Rebus.MySql.Tests
{
    public static class MySqlTestHelper
    {
        const string TableDoesNotExist = "42S02";

        public static void DropTable(string tableName)
        {
            using (var connection = MySqlConnectionHelper.GetConnection().Result)
            {
                using (var comand = connection.CreateCommand())
                {
                    comand.CommandText = $@"drop table ""{tableName}"";";

                    try
                    {
                        comand.ExecuteNonQuery();

                        Console.WriteLine("Dropped postgres table '{0}'", tableName);
                    }
                    catch (MySqlException exception) when (exception.SqlState == TableDoesNotExist)
                    {
                    }
                }

                connection.Complete();
            }
        }

        static string GetConnectionStringForDatabase(string databaseName)
        {
            return Environment.GetEnvironmentVariable("REBUS_MYSQL")
                   ?? $"server=localhost; database={databaseName}; user id=mysql; password=mysql;maximum pool size=30;";
        }
    }
}