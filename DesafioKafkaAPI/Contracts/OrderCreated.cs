namespace DesafioKafkaAPI.Contracts
{
    public record OrderCreated(Guid OrderId, int CustomerId, DateTime CreatedAt, IReadOnlyList<OrderItem> Items);
}
