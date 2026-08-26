using DesafioKafkaAPI.Contracts;

namespace DesafioKafkaAPI.Orders
{
    public record CreateOrderRequest(int CustomerId, List<OrderItem> Items);
}
