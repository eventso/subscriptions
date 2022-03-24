using System;
using System.Threading.Tasks;
using Npgsql;

namespace Eventso.Subscription.Inbox
{
    public sealed class InboxWriter
    {
        private readonly string _connectionString;
        private readonly int? _limit;

        public InboxWriter(string connectionString, int? limit)
        {
            _connectionString = connectionString;
            _limit = limit;
        }

        //public async Task<bool> TryEnqueue(string schema, EventRecord record)
        //{

        //}


        public async Task Add(string schema, EventRecord record)
        {
            using (var connection = new NpgsqlConnection(_connectionString))
            {
                await connection.OpenWithSchemaAsync(schema);

                if (_limit.HasValue)
                    await CheckLimit(connection, schema);

                using (var command = new NpgsqlCommand
                {
                    Parameters =
                    {
                        new NpgsqlParameter<Guid>("streamId", record.StreamId),
                        new NpgsqlParameter<long>("number", record.Number),
                        new NpgsqlParameter<string>("error", record.Error),
                        new NpgsqlParameter<byte[]>("body", record.Body)
                    },
                    Connection = connection,
                    CommandText = @"insert into messages 
                        (stream_id, number, body, error)
                        values(@streamId, @number, @body, @error)
                        ON CONFLICT (stream_id, number) DO NOTHING"
                })
                {
                    await command.PrepareAsync();
                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        private async Task<bool> Contains(string schema, Guid streamId)
        {
            using (var connection = new NpgsqlConnection(_connectionString))
            {
                await connection.OpenWithSchemaAsync(schema);

                using (var command = BuildCheckCommand(connection, streamId))
                {
                    await command.PrepareAsync();

                    var result = (int)await command.ExecuteScalarAsync();

                    return result > 0;
                }
            }
        }

        private NpgsqlCommand BuildCheckCommand(NpgsqlConnection connection, Guid streamId)
        {
            return new NpgsqlCommand
            {
                Parameters =
                {
                    new NpgsqlParameter<Guid>("streamId", streamId),
                },
                Connection = connection,
                CommandText = @"SELECT COUNT(*) FROM messages WHERE stream_id=@streamId"
            };
        }

        private async Task CheckLimit(NpgsqlConnection connection, string schema)
        {
            using (var command = new NpgsqlCommand
            {
                Connection = connection,
                CommandText = @"select count(*) from messages"
            })
            {
                await command.PrepareAsync();
                var count = (long) await command.ExecuteScalarAsync();

                if (count >= _limit)
                    throw new InvalidOperationException($"Inbox {schema} limit {_limit} exceeded ");
            }
        }
    }
}