using System.Data;

namespace DesafioKafkaAPI.Data
{
    public interface IDbConnectionFactory
    {
        IDbConnection CreateOpenConnection();
    }
}
