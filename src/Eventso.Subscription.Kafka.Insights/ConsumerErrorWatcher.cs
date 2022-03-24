using System;
using System.Threading;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.Insights
{
    internal sealed class ConsumerErrorWatcher : IDisposable
    {
        private readonly CancellationTokenSource _tokenSource = new();

        public Error Error { get; private set; }

        public CancellationToken OnErrorToken => _tokenSource.Token;

        public void SetError(Error error)
        {
            Error = error;
            _tokenSource.Cancel();
        }

        public void Dispose()
            => _tokenSource.Dispose();
    }
}