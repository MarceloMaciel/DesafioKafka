namespace DesafioKafkaAPI.Messaging
{
    public static class KafkaTopics
    {
        public const string OrderCreated = "order-created";
        public const string OrderCreatedDlq = "order-created.DLQ";

        public const int OrderCreatedPartitions = 3;
    }
}
