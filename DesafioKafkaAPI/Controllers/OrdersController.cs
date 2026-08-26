using System.Text.Json;
using DesafioKafkaAPI.Contracts;
using DesafioKafkaAPI.Messaging;
using DesafioKafkaAPI.Orders;
using Microsoft.AspNetCore.Mvc;

namespace DesafioKafkaAPI.Controllers
{
    [ApiController]
    [Route("orders")]
    public class OrdersController : ControllerBase
    {
        private readonly IOrderRepository _orderRepository;
        private readonly IKafkaProducer _kafkaProducer;

        public OrdersController(IOrderRepository orderRepository, IKafkaProducer kafkaProducer)
        {
            _orderRepository = orderRepository;
            _kafkaProducer = kafkaProducer;
        }

        [HttpPost]
        public async Task<IActionResult> Post([FromBody] CreateOrderRequest request, CancellationToken cancellationToken)
        {
            var order = new Order(Guid.NewGuid(), request.CustomerId, DateTime.UtcNow, request.Items);

            await _orderRepository.AddAsync(order, cancellationToken);

            var orderCreated = new OrderCreated(order.OrderId, order.CustomerId, order.CreatedAt, order.Items);
            var payload = JsonSerializer.Serialize(orderCreated);

            // Chave = OrderId (§3.4): garante que mensagens do mesmo pedido caiam sempre na mesma partição.
            await _kafkaProducer.PublishAsync(KafkaTopics.OrderCreated, order.OrderId.ToString(), payload, cancellationToken);

            return Accepted(new { orderId = order.OrderId });
        }

        // Conveniência para validar a persistência da Fase 1 (não está no brief, mas ajuda a conferir o .db).
        [HttpGet("{orderId:guid}")]
        public async Task<IActionResult> GetById(Guid orderId, CancellationToken cancellationToken)
        {
            var order = await _orderRepository.GetByIdAsync(orderId, cancellationToken);

            return order is null ? NotFound() : Ok(order);
        }
    }
}
