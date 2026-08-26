using DesafioKafkaAPI.Contracts;

namespace DesafioKafkaAPI.Orders
{
    public record Order(Guid OrderId, int CustomerId, DateTime CreatedAt, IReadOnlyList<OrderItem> Items);
}
