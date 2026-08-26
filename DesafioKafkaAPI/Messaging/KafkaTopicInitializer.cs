using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace DesafioKafkaAPI.Messaging
{
    // Cria os tópicos automaticamente na subida (decisão §7.3: mais cômodo para o estudo do que criar
    // manualmente pela Kafka UI). order-created sai com várias partições de propósito (ver KafkaTopics).
    public static class KafkaTopicInitializer
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

        // Roda em background (fire-and-forget): sem broker no ar, o AdminClient do librdkafka pode ficar
        // minutos tentando buscar metadata antes de desistir sozinho, o que travaria o boot da app inteira.
        // Aqui aplicamos nosso próprio timeout curto e deixamos a app subir normalmente enquanto isso.
        public static void EnsureTopicsCreatedInBackground(IConfiguration configuration, ILogger logger)
        {
            _ = Task.Run(() => EnsureTopicsCreatedAsync(configuration, logger));
        }

        private static async Task EnsureTopicsCreatedAsync(IConfiguration configuration, ILogger logger)
        {
            var bootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:9092";

            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();

            var topics = new[]
            {
                new TopicSpecification { Name = KafkaTopics.OrderCreated, NumPartitions = KafkaTopics.OrderCreatedPartitions, ReplicationFactor = 1 },
                new TopicSpecification { Name = KafkaTopics.OrderCreatedDlq, NumPartitions = 1, ReplicationFactor = 1 }
            };
            var topicNames = string.Join(", ", topics.Select(t => t.Name));

            try
            {
                var createTask = admin.CreateTopicsAsync(topics, new CreateTopicsOptions { RequestTimeout = Timeout });
                var finished = await Task.WhenAny(createTask, Task.Delay(Timeout));

                if (finished != createTask)
                {
                    logger.LogWarning(
                        "Kafka em {BootstrapServers} não respondeu em {Timeout}s ao criar tópicos ({Topics}). A app segue; publicar eventos vai falhar até o broker estar disponível.",
                        bootstrapServers, Timeout.TotalSeconds, topicNames);
                    return;
                }

                await createTask; // propaga eventual exceção já concluída
                logger.LogInformation("Tópicos Kafka verificados/criados: {Topics}", topicNames);
            }
            catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
            {
                logger.LogInformation("Tópicos Kafka já existiam: {Topics}", topicNames);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Não foi possível criar/verificar os tópicos Kafka em {BootstrapServers} ({Topics}).", bootstrapServers, topicNames);
            }
        }
    }
}
