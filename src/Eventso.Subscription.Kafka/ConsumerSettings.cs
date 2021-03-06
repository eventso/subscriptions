using System;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka
{
    public sealed class ConsumerSettings
    {
        public ConsumerSettings()
        {
            Config = new ConsumerConfig
            {
                EnableAutoCommit = false,
            };
        }

        public ConsumerSettings(
            string brokers,
            string groupId,
            TimeSpan? maxPollInterval = default,
            TimeSpan? sessionTimeout = default,
            AutoOffsetReset autoOffsetReset = AutoOffsetReset.Earliest) : this()
        {
            Config.BootstrapServers = brokers;
            Config.GroupId = groupId;
            Config.AutoOffsetReset = autoOffsetReset;

            if (maxPollInterval.HasValue)
                Config.MaxPollIntervalMs = (int) maxPollInterval.Value.TotalMilliseconds;

            if (sessionTimeout.HasValue)
                Config.SessionTimeoutMs = (int) sessionTimeout.Value.TotalMilliseconds;
        }

        public ConsumerConfig Config { get; }

        public string Topic { get; set; }

        /// <summary>
        /// Note: Timed batch creation is based on this timeout. 
        /// </summary>
        public TimeSpan ConsumeTimeout { get; set; } = TimeSpan.FromMilliseconds(500);
    }
}