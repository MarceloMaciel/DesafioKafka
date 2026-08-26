using Confluent.Kafka;

namespace DesafioKafkaAPI.Messaging
{
    // Evolução do KafkaProducer do KafkaEstudoInicial: <Null,string> vira <string,string>
    // (chave = OrderId, valor = JSON), com Acks.All + idempotência do producer (ver brief §3.3).
    public class KafkaProducer : IKafkaProducer, IDisposable
    {
        private readonly IProducer<string, string> _producer;
        private readonly ILogger<KafkaProducer> _logger;

        public KafkaProducer(IConfiguration configuration, ILogger<KafkaProducer> logger)
        {
            _logger = logger;

            var config = new ProducerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:9092",
                Acks = Acks.All,
                EnableIdempotence = true,
                MessageSendMaxRetries = 3,
                RetryBackoffMs = 100,
                // Default é 5 min (300000ms) — bom demais pra estudo: sem broker, POST /orders ficaria
                // pendurado por minutos. 10s já dá pra ver o retry acontecer sem travar o teste manual.
                MessageTimeoutMs = 10000
            };

            _producer = new ProducerBuilder<string, string>(config).Build();
        }

        public async Task PublishAsync(string topic, string key, string value, CancellationToken cancellationToken = default)
        {
            var message = new Message<string, string> { Key = key, Value = value };

            var result = await _producer.ProduceAsync(topic, message, cancellationToken);

            _logger.LogInformation(
                "Mensagem publicada em {Topic} partição {Partition} offset {Offset} chave {Key}",
                result.Topic, result.Partition.Value, result.Offset.Value, key);
        }

        public void Dispose()
        {
            // Garante que mensagens em voo sejam entregues antes de derrubar o producer.
            _producer.Flush(TimeSpan.FromSeconds(10));
            _producer.Dispose();
        }
    }
}
