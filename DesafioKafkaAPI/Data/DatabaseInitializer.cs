namespace DesafioKafkaAPI.Data
{
    public static class DatabaseInitializer
    {
        public static void EnsureCreated(IDbConnectionFactory connectionFactory)
        {
            using var connection = connectionFactory.CreateOpenConnection();

            using (var pragmaCommand = connection.CreateCommand())
            {
                // WAL reduz travas de "database is locked" com escrita concorrente
                pragmaCommand.CommandText = "PRAGMA journal_mode=WAL;";
                pragmaCommand.ExecuteNonQuery();
            }

            using var createTableCommand = connection.CreateCommand();
            createTableCommand.CommandText = """
                CREATE TABLE IF NOT EXISTS Orders (
                    OrderId TEXT PRIMARY KEY,
                    CustomerId INTEGER NOT NULL,
                    CreatedAt TEXT NOT NULL,
                    ItemsJson TEXT NOT NULL
                );
                """;
            createTableCommand.ExecuteNonQuery();
        }
    }
}
