using DesafioKafkaAPI.Data;
using DesafioKafkaAPI.Orders;

namespace DesafioKafkaAPI
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            // Add services to the container.

            builder.Services.AddControllers();
            // Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
            builder.Services.AddOpenApi();

            builder.Services.AddSingleton<IDbConnectionFactory, SqliteConnectionFactory>();
            builder.Services.AddScoped<IOrderRepository, OrderRepository>();

            var app = builder.Build();

            // Schema do SQLite: sem migrations, roda uma vez na subida (ver Data/DatabaseInitializer.cs).
            DatabaseInitializer.EnsureCreated(app.Services.GetRequiredService<IDbConnectionFactory>());

            // Configure the HTTP request pipeline.
            if (app.Environment.IsDevelopment())
            {
                app.MapOpenApi();
            }

            app.UseHttpsRedirection();

            app.UseAuthorization();


            app.MapControllers();

            app.Run();
        }
    }
}
