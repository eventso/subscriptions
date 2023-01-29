using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.Insights;

internal sealed class KafkaConsumer<T> : IDisposable
{
    private readonly IConsumer<string, T> _consumer;
    private readonly ConsumerErrorWatcher _errorWatcher;

    public KafkaConsumer(IConsumer<string, T> consumer, ConsumerErrorWatcher errorWatcher)
    {
        _consumer = consumer;
        _errorWatcher = errorWatcher;
    }

    public ConsumeResult<string, T> Consume(TopicPartitionOffset offset, CancellationToken token)
    {
        try
        {
            _consumer.Assign(offset);

            using var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(
                _errorWatcher.OnErrorToken, token);

            try
            {
                var result = _consumer.Consume(tokenSource.Token);

                if (result != null && !result.TopicPartitionOffset.Equals(offset))
                    throw new InvalidOperationException("Consumed message offset doesn't match requested one.");

                return result;
            }
            catch (OperationCanceledException)
            {
                if (_errorWatcher.OnErrorToken.IsCancellationRequested)
                    throw new KafkaException(_errorWatcher.Error);

                throw;
            }
        }
        finally
        {
            _consumer.Unassign();
        }
    }

    public void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
        _errorWatcher.Dispose();
    }
}