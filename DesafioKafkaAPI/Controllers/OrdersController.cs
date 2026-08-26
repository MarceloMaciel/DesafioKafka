using DesafioKafkaAPI.Orders;
using Microsoft.AspNetCore.Mvc;

namespace DesafioKafkaAPI.Controllers
{
    [ApiController]
    [Route("orders")]
    public class OrdersController : ControllerBase
    {
        private readonly IOrderRepository _orderRepository;

        public OrdersController(IOrderRepository orderRepository)
        {
            _orderRepository = orderRepository;
        }

        [HttpPost]
        public async Task<IActionResult> Post([FromBody] CreateOrderRequest request, CancellationToken cancellationToken)
        {
            var order = new Order(Guid.NewGuid(), request.CustomerId, DateTime.UtcNow, request.Items);

            await _orderRepository.AddAsync(order, cancellationToken);

            // Fase 2 publica o evento OrderCreated aqui.

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
