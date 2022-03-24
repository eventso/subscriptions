using System;

namespace Eventso.Subscription.Inbox
{
    public sealed class InboxConfiguration
    {
        public static readonly InboxConfiguration Disabled = new InboxConfiguration(false);

        public string Schema { get; }
        public bool Enabled { get; }

        public InboxConfiguration(string schema)
            : this(true)
        {
            if (string.IsNullOrWhiteSpace(schema))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(schema));

            Schema = schema;
        }

        private InboxConfiguration(bool enabled)
        {
            Enabled = enabled;
        }
    }
}