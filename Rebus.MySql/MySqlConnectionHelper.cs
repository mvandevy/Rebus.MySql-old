using System.Data;
using System.Threading.Tasks;
using MySqlData = MySql.Data;

namespace Rebus.MySql
{
    public class MySqlConnectionHelper
    {
        readonly string _connectionString;

        public MySqlConnectionHelper(string connectionString)
        {
            _connectionString = connectionString;
        }

        /// <summary>
        /// Gets a fresh, open and ready-to-use connection wrapper
        /// </summary>
        public async Task<MySqlConnection> GetConnection()
        {
            var connection = new MySqlData.MySqlClient.MySqlConnection(_connectionString);

            await connection.OpenAsync();

            var currentTransaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);

            return new MySqlConnection(connection, currentTransaction);
        }
    }
}