using System;
using System.Data;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Swisschain.Extensions.Postgres.StaleConnectionsCleaning
{
    public sealed class StaleConnectionsCleaner : IStaleConnectionsCleaner
    {
        private readonly ILogger<StaleConnectionsCleaner> _logger;
        private readonly string _connectionString;

        public StaleConnectionsCleaner(ILogger<StaleConnectionsCleaner> logger, string connectionString)
        {
            _logger = logger;
            _connectionString = connectionString;
        }

        public async Task Clear()
        {
            const string script = @"WITH inactive_connections AS 
            (
                SELECT
                    pid,
                    rank() over (partition by client_addr order by backend_start ASC) as rank
                FROM 
                    pg_stat_activity
                WHERE
                    -- Exclude the thread owned connection (ie no auto-kill)
                    pid <> pg_backend_pid()
                AND
                    -- Exclude known applications connections
                    application_name !~ '(?:psql)|(?:pgAdmin.+)'
                AND
                    -- Include connections to the same database the thread is connected to
                    datname = current_database() 
                AND
                    -- Include connections of the same user the thread is connected via
                    usename = current_user
                AND
                    -- Include inactive connections only
                    state in ('idle', 'idle in transaction', 'idle in transaction (aborted)', 'disabled') 
                AND
                    -- Include old connections (found with the state_change field)
                    current_timestamp - state_change > interval '5 minutes' 
            )
            SELECT
                pg_terminate_backend(pid)
            FROM
                inactive_connections 
            WHERE
                rank > 1";

            await using var connection = new NpgsqlConnection(_connectionString);
            
            _logger.LogInformation("Stale DB connections cleaning is being started {@context}...",
                new
                {
                    DataBaseServer = connection.DataSource,
                    DataBase = connection.Database
                });

            try
            {
                if (connection.State != ConnectionState.Open)
                {
                    await connection.OpenAsync();
                }

                await using var command = connection.CreateCommand();

                command.CommandText = script;

                var cleanedConnectionsCount = await command.ExecuteNonQueryAsync();

                _logger.LogInformation("Stale DB connections cleaning is being started {@context}...",
                    new
                    {
                        DataBaseServer = connection.DataSource,
                        DataBase = connection.Database,
                        CleanedConnectionsCount = cleanedConnectionsCount
                    });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Stale DB connections cleaning has been failed {@context}",
                    new
                    {
                        DataBaseServer = connection.DataSource,
                        DataBase = connection.Database
                    });

                throw;
            }
        }
    }
}
