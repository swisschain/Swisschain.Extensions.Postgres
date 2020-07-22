using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Swisschain.Extensions.Postgres.StaleConnectionsCleaning;

namespace Swisschain.Extensions.Postgres
{
    public static class StaleConnectionsCleaningServiceCollectionExtensions
    {
        public static IServiceCollection AddStaleConnectionsCleaning(this IServiceCollection services, string connectionString)
        {
            return services.AddHostedService(c =>
                new StaleConnectionsCleanerHost(
                    c.GetRequiredService<ILogger<StaleConnectionsCleanerHost>>(),
                    new StaleConnectionsCleaner(
                        c.GetRequiredService<ILogger<StaleConnectionsCleaner>>(),
                        connectionString)));
        }
    }
}
