using System.Text.Json;
using Dapper;
using DesafioKafkaAPI.Contracts;
using DesafioKafkaAPI.Data;

namespace DesafioKafkaAPI.Orders
{
    public class OrderRepository : IOrderRepository
    {
        private readonly IDbConnectionFactory _connectionFactory;

        public OrderRepository(IDbConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;
        }

        public async Task AddAsync(Order order, CancellationToken cancellationToken = default)
        {
            using var connection = _connectionFactory.CreateOpenConnection();

            const string sql = """
                INSERT INTO Orders (OrderId, CustomerId, CreatedAt, ItemsJson)
                VALUES (@OrderId, @CustomerId, @CreatedAt, @ItemsJson);
                """;

            await connection.ExecuteAsync(sql, new
            {
                OrderId = order.OrderId.ToString(),
                order.CustomerId,
                CreatedAt = order.CreatedAt.ToString("O"),
                ItemsJson = JsonSerializer.Serialize(order.Items)
            });
        }

        public async Task<Order?> GetByIdAsync(Guid orderId, CancellationToken cancellationToken = default)
        {
            using var connection = _connectionFactory.CreateOpenConnection();

            const string sql = "SELECT * FROM Orders WHERE OrderId = @OrderId;";

            var row = await connection.QueryFirstOrDefaultAsync(sql, new { OrderId = orderId.ToString() });
            if (row is null)
            {
                return null;
            }

            var items = JsonSerializer.Deserialize<List<OrderItem>>((string)row.ItemsJson) ?? [];

            return new Order(
                Guid.Parse((string)row.OrderId),
                (int)(long)row.CustomerId,
                DateTime.Parse((string)row.CreatedAt).ToUniversalTime(),
                items);
        }
    }
}
