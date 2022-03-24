using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Eventso.Subscription.Inbox
{
    public sealed class BackgroundInboxPublisher : BackgroundService
    {
        private readonly string _connectionString;
        private readonly IReadOnlyCollection<string> _schemas;
        private readonly InboxPublisher _publisher;
        private readonly TimeSpan _interval;
        private readonly ILogger<BackgroundInboxPublisher> _logger;

        public BackgroundInboxPublisher(
            string connectionString,
            IReadOnlyCollection<string> schemas,
            InboxPublisher publisher,
            TimeSpan interval,
            ILogger<BackgroundInboxPublisher> logger)
        {
            _connectionString = connectionString;
            _schemas = schemas;
            _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
            _interval = interval;
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await Publish(stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Inbox publishing failed");
                }

                await Task.Delay(_interval, stoppingToken);
            }
        }

        private async Task Publish(CancellationToken token)
        {
            using (var connection = new NpgsqlConnection(_connectionString))
            {
                await connection.OpenAsync(token);

                foreach (var schema in _schemas)
                {
                    await connection.SwitchSchema(schema);

                    await PublishStreams(connection, token);
                }
            }
        }

        private async Task PublishStreams(NpgsqlConnection connection, CancellationToken token)
        {
            var aggregateException = new List<Exception>();

            using (var command = new NpgsqlCommand( 
                "select distinct stream_id from messages order by created",
                connection
            ))
            {
                await command.PrepareAsync(token);

                using (var reader = await command.ExecuteReaderAsync(token))
                {
                    while (await reader.ReadAsync(token))
                    {
                        try
                        {
                            await _publisher.TryPublish(connection, reader.GetGuid(0), token);
                        }
                        catch (Exception ex)
                        {
                            aggregateException.Add(ex);
                        }
                    }
                }
            }

            if (aggregateException.Count > 0)
            {
                throw new AggregateException(aggregateException.TakeLast(10));
            }
        }

       
    }
    
}
