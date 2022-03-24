using System.Threading.Tasks;
using Npgsql;

namespace Eventso.Subscription.Inbox
{
    public static class ConnectionExtensions
    {
        public static async Task OpenWithSchemaAsync(this NpgsqlConnection connection, string defaultSchema)
        {
            await connection.OpenAsync();

            await SwitchSchema(connection, defaultSchema);
        }

        public static async Task SwitchSchema(this NpgsqlConnection connection, string defaultSchema)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SET SCHEMA '" + defaultSchema + "'";
                await command.ExecuteNonQueryAsync();
            }
        }
    }
}