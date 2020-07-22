using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Swisschain.Extensions.Postgres.StaleConnectionsCleaning
{
    internal class StaleConnectionsCleanerHost : IHostedService
    {
        private readonly ILogger<StaleConnectionsCleanerHost> _logger;
        private readonly IStaleConnectionsCleaner _staleConnectionsCleaner;

        public StaleConnectionsCleanerHost(ILogger<StaleConnectionsCleanerHost> logger, IStaleConnectionsCleaner staleConnectionsCleaner)
        {
            _logger = logger;
            _staleConnectionsCleaner = staleConnectionsCleaner;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                await _staleConnectionsCleaner.Clear();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to clean stale connections");
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
