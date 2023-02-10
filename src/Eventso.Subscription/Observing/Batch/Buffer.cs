using System.Runtime.InteropServices;
using System.Threading.Channels;

namespace Eventso.Subscription.Observing.Batch;

internal sealed class Buffer<TEvent> : IDisposable
{
    private readonly int _maxBatchSize;
    private readonly int _maxBufferSize;
    private readonly TimeSpan _timeout;
    private readonly Channel<Batch> _target;
    private readonly CancellationTokenSource _tokenSource;
    private readonly Channel<BufferAction> _channel;
    private readonly Task _readingTask;

    private readonly Timer _timer;
    private int _timerStartVersion;
    private int _version;

    private PooledList<BufferedEvent> _events;
    private int _toBeHandledEventsCount;

    private bool _disposed;

    public Buffer(
        int maxBatchSize,
        TimeSpan timeout,
        Channel<Batch> target,
        int maxBufferSize,
        CancellationToken token)
    {
        if (maxBatchSize <= 1)
            throw new ArgumentException("Buffer size must be greater than 1.");

        _maxBatchSize = maxBatchSize;
        _maxBufferSize = maxBufferSize;

        _timeout = timeout;
        _target = target;
        _tokenSource = CancellationTokenSource.CreateLinkedTokenSource(token);

        _events = new PooledList<BufferedEvent>(maxBufferSize);
        _channel = Channel.CreateBounded<BufferAction>(
            new BoundedChannelOptions(1)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait,
            });

        _readingTask = Task.Run(BeginReadChannel);

        _timer = new Timer(TriggerTimer);
    }

    public async Task Add(TEvent message, bool skipped, CancellationToken token)
    {
        CheckDisposed();

        if (_readingTask.IsCanceled || _tokenSource.IsCancellationRequested)
            throw new OperationCanceledException("Buffer is cancelled");

        if (_target.Reader.Completion.IsFaulted)
            await _target.Reader.Completion;

        if (_readingTask.IsFaulted)
            await _readingTask;

        if (_readingTask.IsCompleted || _target.Reader.Completion.IsCompleted)
            throw new InvalidOperationException("Buffer already completed.");

        await _channel.Writer.WriteAsync(
            new BufferAction(new BufferedEvent(message, skipped)),
            token);
    }

    public async Task Complete()
    {
        CheckDisposed();

        _channel.Writer.TryComplete();

        if (!_readingTask.IsCanceled)
            await _readingTask;

        await _channel.Reader.Completion;

        await TriggerSend();
    }

    public void Dispose()
    {
        _disposed = true;

        if (!_tokenSource.IsCancellationRequested)
            _tokenSource.Cancel();

        _timer.Dispose();
        _channel.Writer.TryComplete();
        _events.Dispose();

        _tokenSource.Dispose();
    }

    private async Task BeginReadChannel()
    {
        try
        {
            await foreach (var action in _channel.Reader.ReadAllAsync(_tokenSource.Token))
            {
                await Process(action);
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

    private Task Process(in BufferAction action)
    {
        if (action.IsTimeout)
        {
            return action.Version == _version
                ? TriggerSend()
                : Task.CompletedTask;
        }

        _events.Add(action.Event);

        if (_events.Count == 1)
            StartTimer();

        if (!action.Event.Skipped)
            ++_toBeHandledEventsCount;

        if (_toBeHandledEventsCount >= _maxBatchSize ||
            _events.Count >= _maxBufferSize)
            return TriggerSend();

        return Task.CompletedTask;
    }

    private void StartTimer()
    {
        _timerStartVersion = _version;
        _timer.Change(_timeout, Timeout.InfiniteTimeSpan);
    }

    private void TriggerTimer(object state) =>
        _ = _channel.Writer.WriteAsync(new BufferAction(_timerStartVersion), _tokenSource.Token);

    private async Task TriggerSend()
    {
        if (_events.Count == 0)
            return;

        _timer.Change(Timeout.Infinite, Timeout.Infinite);

        var batch = new Batch(_events, _toBeHandledEventsCount);

        ++_version;
        _events = new PooledList<BufferedEvent>(_maxBatchSize);
        _toBeHandledEventsCount = 0;

        await _target.Writer.WriteAsync(batch, _tokenSource.Token);
    }

    private void CheckDisposed()
    {
        if (_disposed) throw new ObjectDisposedException("Buffer disposed");
    }

    [StructLayout(LayoutKind.Auto)]
    public readonly struct BufferedEvent
    {
        public readonly TEvent Event;
        public readonly bool Skipped;

        public BufferedEvent(TEvent @event, bool skipped)
        {
            Event = @event;
            Skipped = skipped;
        }
    }

    public readonly struct Batch
    {
        public readonly PooledList<BufferedEvent> Events;
        public readonly int ToBeHandledEventCount;

        public Batch(PooledList<BufferedEvent> events, int toBeHandledEventCount)
        {
            Events = events;
            ToBeHandledEventCount = toBeHandledEventCount;
        }
    }

    [StructLayout(LayoutKind.Auto)]
    private readonly struct BufferAction
    {
        public readonly BufferedEvent Event;
        public readonly int Version;
        public readonly bool IsTimeout;

        public BufferAction(BufferedEvent @event)
        {
            Event = @event;
            Version = 0;
            IsTimeout = false;
        }

        public BufferAction(int version)
        {
            Event = default;
            Version = version;
            IsTimeout = true;
        }
    }
}