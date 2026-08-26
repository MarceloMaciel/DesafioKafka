using System.Data;
using Microsoft.Data.Sqlite;

namespace DesafioKafkaAPI.Data
{
    public class SqliteConnectionFactory : IDbConnectionFactory
    {
        private readonly string _connectionString;

        public SqliteConnectionFactory(IConfiguration configuration)
        {
            _connectionString = configuration.GetConnectionString("Default")
                ?? "Data Source=pedidos.db";
        }

        public IDbConnection CreateOpenConnection()
        {
            var connection = new SqliteConnection(_connectionString);
            connection.Open();
            return connection;
        }
    }
}
