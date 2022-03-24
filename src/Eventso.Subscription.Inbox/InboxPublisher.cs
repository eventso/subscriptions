using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace Eventso.Subscription.Inbox
{
    public sealed class InboxPublisher
    {
        private readonly IMessagePublisher _publisher;

        public InboxPublisher(IMessagePublisher publisher)
        {
            _publisher = publisher;
        }

        public async Task<bool> TryPublish(
            NpgsqlConnection connection, 
            Guid streamId,
            CancellationToken token)
        {
            var lockId = streamId.GetLockId();

            if (!await TryLock(connection, lockId))
                return false;

            try
            {
                await Publish(connection, streamId);
                return true;
            }
            finally
            {
                await Unlock(lockId, connection);
            }
        }

        private async Task Publish(NpgsqlConnection connection, Guid streamId)
        {
            var topMessage = await ReadStreamTop(connection, streamId);

            while (topMessage.HasValue)
            {
                try
                {
                    await _publisher.Publish(topMessage.Value.body);

                    await PublishSucceeded(connection, streamId, topMessage.Value.number);
                }
                catch (Exception e)
                {
                    await PublishFail(connection, streamId, topMessage.Value.number, e.ToString());
                    throw;
                }
            }
        }

        private async Task<bool> TryLock(NpgsqlConnection connection, long lockId)
        {
            const string tryLock = "SELECT pg_try_advisory_lock(@lock)";

            using (var command = new NpgsqlCommand(tryLock, connection))
            {
                command.Parameters.Add(new NpgsqlParameter<long>("lock", lockId));

                await command.PrepareAsync();

                return (bool) await command.ExecuteScalarAsync();
            }
        }

        private async Task Unlock(long lockId, NpgsqlConnection connection)
        {
            const string unlock = "SELECT pg_advisory_unlock(@lock);";

            using (var command = new NpgsqlCommand(unlock, connection))
            {
                command.Parameters.Add(new NpgsqlParameter<long>("lock", lockId));

                await command.PrepareAsync();

                await command.ExecuteScalarAsync();
            }
        }

        private async Task<(long number, byte[] body)?> ReadStreamTop(NpgsqlConnection connection, Guid streamId)
        {
            using (var command = new NpgsqlCommand
            {
                Parameters =
                {
                    new NpgsqlParameter<Guid>("streamId", streamId),
                },
                Connection = connection,
                CommandText = "SELECT number, body FROM messages WHERE stream_id = @streamId ORDER BY number LIMIT 1"
            })
            {
                await command.PrepareAsync();

                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                        return (reader.GetFieldValue<long>(0), reader.GetFieldValue<byte[]>(1));

                    return default;
                }
            }
        }

        private async Task PublishSucceeded(NpgsqlConnection connection, Guid streamId, long number)
        {
            using (var command = new NpgsqlCommand
            {
                Parameters =
                {
                    new NpgsqlParameter<Guid>("streamId", streamId),
                    new NpgsqlParameter<long>("number", number),
                },
                Connection = connection,
                CommandText = "DELETE FROM messages WHERE stream_id = @streamId and number = @number"
            })
            {
                var attempt = 0;
                while (attempt < 6)
                {
                    attempt++;

                    try
                    {
                        await command.PrepareAsync();
                        await command.ExecuteNonQueryAsync();
                        return;
                    }
                    catch
                    {
                        if (attempt > 5)
                            throw;
                    }
                }
            }
        }

        private async Task PublishFail(NpgsqlConnection connection,
            Guid streamId,
            long number,
            string exception)
        {
            using (var command = new NpgsqlCommand
            {
                Parameters =
                {
                    new NpgsqlParameter<Guid>("streamId", streamId),
                    new NpgsqlParameter<long>("number", number),
                    new NpgsqlParameter<string>("exception", exception)
                },
                Connection = connection,
                CommandText = @"update messages 
                    SET error = @exception, 
                        attempts = attempts + 1,
                        updated = now()
                    WHERE stream_id = @streamId and number = @number"
            })
            {
                await command.PrepareAsync();
                await command.ExecuteNonQueryAsync();
            }
        }
    }
}