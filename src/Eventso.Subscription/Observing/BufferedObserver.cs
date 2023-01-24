using System.Threading.Channels;

namespace Eventso.Subscription.Observing;

public sealed class BufferedObserver<T> : IObserver<T>, IDisposable
    where T : IEvent
{
    private readonly IObserver<T> _nextObserver;
    private readonly CancellationToken _consumerCancellationToken;
    private readonly Channel<T> _channel;
    private readonly Task _readingTask;

    public BufferedObserver(int capacity, IObserver<T> nextObserver, CancellationToken consumerCancellationToken)
    {
        _nextObserver = nextObserver;
        _consumerCancellationToken = consumerCancellationToken;
        _channel = Channel.CreateBounded<T>(
            new BoundedChannelOptions(capacity)
            {
                SingleReader = true,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.Wait,
            });

        _readingTask = Task.Run(BeginReadChannel);
    }

    public async Task OnEventAppeared(T @event, CancellationToken token)
    {
        if (_readingTask.IsCanceled || _consumerCancellationToken.IsCancellationRequested)
            throw new OperationCanceledException("Buffered observer is cancelled");

        if (_readingTask.IsFaulted)
            await _readingTask;

        if (_readingTask.IsCompleted)
            throw new InvalidOperationException("Buffered observer already completed.");

        await _channel.Writer.WriteAsync(@event, token);
    }

    public Task Complete()
    {
        _channel.Writer.TryComplete();
        return Task.WhenAll(_channel.Reader.Completion, _readingTask);
    }

    public void Dispose()
    {
        _channel.Writer.TryComplete();
        (_nextObserver as IDisposable)?.Dispose();
    }

    private async Task BeginReadChannel()
    {
        try
        {
            await foreach (var @event in _channel.Reader.ReadAllAsync(_consumerCancellationToken))
            {
                await _nextObserver.OnEventAppeared(@event, _consumerCancellationToken);
            }
        }
        catch (Exception ex)
        {
            _channel.Writer.TryComplete(ex);

            //cleanup queue
            while (_channel.Reader.TryRead(out _))
            {
            }

            throw;
        }
    }
}