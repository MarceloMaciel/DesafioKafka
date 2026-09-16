namespace DesafioKafkaAPI.Messaging
{
    // GroupIds centralizados — um por consumer (§3.5). Cada GroupId distinto recebe sua própria cópia
    // de cada evento (fan-out): é isso que separa "serviços" dentro do mesmo processo.
    public static class KafkaConsumerGroups
    {
        public const string Notificacao = "notificacao";
    }
}
