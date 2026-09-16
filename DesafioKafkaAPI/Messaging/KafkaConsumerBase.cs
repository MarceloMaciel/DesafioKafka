using Confluent.Kafka;

namespace DesafioKafkaAPI.Messaging
{
    // Base reutilizável dos consumers — evolução do loop Subscribe/Consume(token)/Close() do
    // KafkaConsumer original (KafkaEstudoInicial), agora como BackgroundService (§3.5).
    // Cada consumer concreto só implementa "o que fazer com o evento" (HandleMessageAsync).
    public abstract class KafkaConsumerBase : BackgroundService
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger _logger;

        protected KafkaConsumerBase(IConfiguration configuration, ILogger logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        protected abstract string Topic { get; }
        protected abstract string GroupId { get; }

        protected abstract Task HandleMessageAsync(string key, string value, CancellationToken cancellationToken);

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // Consume() do Confluent.Kafka é bloqueante (sem API assíncrona nativa) — roda numa thread dedicada.
            return Task.Run(() => ConsumeLoopAsync(stoppingToken), stoppingToken);
        }

        private async Task ConsumeLoopAsync(CancellationToken stoppingToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _configuration["Kafka:BootstrapServers"] ?? "localhost:9092",
                GroupId = GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false // commit manual após processar -> semântica at-least-once (§3.5)
            };

            using var consumer = new ConsumerBuilder<string, string>(config).Build();
            consumer.Subscribe(Topic);
            _logger.LogInformation("Consumer {GroupId} inscrito em {Topic}, aguardando eventos", GroupId, Topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    ConsumeResult<string, string>? result;
                    try
                    {
                        result = consumer.Consume(stoppingToken);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogWarning(ex, "Falha ao consumir de {Topic} ({GroupId})", Topic, GroupId);
                        continue;
                    }

                    if (result?.Message is null)
                    {
                        continue;
                    }

                    await HandleMessageAsync(result.Message.Key, result.Message.Value, stoppingToken);

                    consumer.Commit(result);
                }
            }
            catch (OperationCanceledException)
            {
                // Shutdown gracioso via stoppingToken do host — integra com o Ctrl+C / stop do host.
            }
            finally
            {
                consumer.Close();
                _logger.LogInformation("Consumer {GroupId} encerrado", GroupId);
            }
        }
    }
}
