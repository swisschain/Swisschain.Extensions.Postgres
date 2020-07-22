using System;
using System.Data;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace Swisschain.Extensions.Postgres.StaleConnectionsCleaning
{
    internal sealed class StaleConnectionsCleaner : IStaleConnectionsCleaner
    {
        private readonly ILogger<StaleConnectionsCleaner> _logger;
        private readonly string _connectionString;
        private readonly TimeSpan _maxAge;

        public StaleConnectionsCleaner(ILogger<StaleConnectionsCleaner> logger, string connectionString, TimeSpan maxAge)
        {
            _logger = logger;
            _connectionString = connectionString;
            _maxAge = maxAge;
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
                    current_timestamp - state_change > @maxAge 
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
                    MaxAge = _maxAge,
                    DatabaseServer = connection.DataSource,
                    Database = connection.Database,
                    UserName = connection.UserName
                });

            try
            {
                if (connection.State != ConnectionState.Open)
                {
                    await connection.OpenAsync();
                }

                await using var command = connection.CreateCommand();

                command.CommandText = script;
                command.Parameters.Add(new NpgsqlParameter("maxAge", NpgsqlDbType.Interval) {Value = _maxAge});

                await using var resultsReader = await command.ExecuteReaderAsync();

                var cleanedConnectionsCount = 0;

                if (resultsReader.HasRows)
                {
                    while (await resultsReader.ReadAsync())
                    {
                        if (resultsReader.GetBoolean(0))
                        {
                            ++cleanedConnectionsCount;
                        }
                    }
                }

                _logger.LogInformation("Stale DB connections cleaning is being started {@context}...",
                    new
                    {
                        MaxAge = _maxAge,
                        DatabaseServer = connection.DataSource,
                        Database = connection.Database,
                        UserName = connection.UserName,
                        CleanedConnectionsCount = cleanedConnectionsCount
                    });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Stale DB connections cleaning has been failed {@context}",
                    new
                    {
                        MaxAge = _maxAge,
                        DataBaseServer = connection.DataSource,
                        Database = connection.Database,
                        UserName = connection.UserName
                    });

                throw;
            }
        }
    }
}
