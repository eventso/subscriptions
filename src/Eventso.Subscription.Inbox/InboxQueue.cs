using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;

namespace Eventso.Subscription.Inbox
{
    public sealed class InboxQueue
    {
        private readonly string _connectionString;
        private HashSet<Guid> _items = new HashSet<Guid>();

        public InboxQueue(string connectionString)
        {
            _connectionString = connectionString;
        }

        public bool Contains(Guid streamId)
        {
            return _items.Contains(streamId);
        }

        public async Task Reload(string schema)
        {
            using (var connection = new NpgsqlConnection(_connectionString))
            {
                await connection.OpenWithSchemaAsync(schema);

                using (var command = new NpgsqlCommand
                {
                    Connection = connection,
                    CommandText = @"SELECT distinct stream_id FROM messages"
                })
                {
                    await command.PrepareAsync();

                    var set = new HashSet<Guid>();
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            set.Add(reader.GetGuid(0));
                        }
                    }

                    _items = set;
                }
            }
        }
    }
}