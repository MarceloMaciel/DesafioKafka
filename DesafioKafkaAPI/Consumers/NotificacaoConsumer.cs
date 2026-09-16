using System.Text.Json;
using DesafioKafkaAPI.Contracts;
using DesafioKafkaAPI.Messaging;

namespace DesafioKafkaAPI.Consumers
{
    // Fase 3: primeiro consumer, o mais simples dos três — só "envia" (loga) uma notificação fake.
    // Foco em observabilidade: log estruturado com propriedades nomeadas, não string interpolada (§3.9).
    public class NotificacaoConsumer : KafkaConsumerBase
    {
        private readonly ILogger<NotificacaoConsumer> _logger;

        public NotificacaoConsumer(IConfiguration configuration, ILogger<NotificacaoConsumer> logger)
            : base(configuration, logger)
        {
            _logger = logger;
        }

        protected override string Topic => KafkaTopics.OrderCreated;
        protected override string GroupId => KafkaConsumerGroups.Notificacao;

        protected override Task HandleMessageAsync(string key, string value, CancellationToken cancellationToken)
        {
            var order = JsonSerializer.Deserialize<OrderCreated>(value);

            _logger.LogInformation(
                "E-mail enviado (fake) — pedido {OrderId} do cliente {CustomerId} processado por {Consumer}",
                order?.OrderId, order?.CustomerId, nameof(NotificacaoConsumer));

            return Task.CompletedTask;
        }
    }
}
