using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka
{
    public sealed class ConsumerAdapter : IConsumer<Message>
    {
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly IConsumer<Guid, ConsumedMessage> _consumer;

        public ConsumerAdapter(
            CancellationTokenSource cancellationTokenSource,
            IConsumer<Guid, ConsumedMessage> consumer)
        {
            _cancellationTokenSource = cancellationTokenSource;
            _consumer = consumer;
            Subscription = string.Join(',', _consumer.Subscription);
        }

        public CancellationToken CancellationToken => _cancellationTokenSource.Token;

        public string Subscription { get; }

        public void Acknowledge(in Message message)
        {
            var offset = new TopicPartitionOffset[]
            {
                new(message.Topic, message.Partition, message.Offset + 1)
            };

            _consumer.Commit(offset);
        }

        public void Acknowledge(IReadOnlyList<Message> messages)
        {
            if (messages.Count == 0)
                return;

            if (messages.Count == 1)
            {
                Acknowledge(messages[0]);
                return;
            }

            _consumer.Commit(GetLatestOffsets(messages));

            static IEnumerable<TopicPartitionOffset> GetLatestOffsets(IReadOnlyList<Message> messages)
            {
                var offsets = new Dictionary<(string, Partition), Offset>(4);

                for (var i = 0; i < messages.Count; i++)
                {
                    var current = messages[i];
                    var isLastMessage = i == messages.Count - 1;

                    if (isLastMessage || !EqualPartition(current, messages[i + 1]))
                    {
                        var key = (current.Topic, current.Partition);
                        offsets[key] = current.Offset;
                    }
                }

                return offsets.Select(o => 
                    new TopicPartitionOffset(o.Key.Item1, o.Key.Item2, o.Value + 1));

                static bool EqualPartition(Message left, Message right)
                    => left.Partition == right.Partition &&
                       left.Topic.Equals(right.Topic);
            }
        }


        public void Cancel() => _cancellationTokenSource.Cancel();
    }
}