using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Eventso.Subscription.Observing.Batch
{
    internal sealed class Buffer<TEvent>
    {
        private readonly int _maxBatchSize;
        private readonly int _maxBufferSize;
        private readonly TimeSpan _timeout;
        private readonly ITargetBlock<Batch> _target;

        private PooledList<BufferedEvent> _events;
        private int _toBeHandledEventsCount;
        private DateTime _batchStartTime;
        private DateTime _bufferStartTime;

        public Buffer(
            int maxBatchSize,
            TimeSpan timeout,
            ITargetBlock<Batch> target,
            int maxBufferSize)
        {
            if (maxBatchSize <= 1)
                throw new ArgumentException("Buffer size must be greater than 1.");

            _maxBatchSize = maxBatchSize;
            _maxBufferSize = maxBufferSize;
            _timeout = timeout;
            _target = target;

            _events = new PooledList<BufferedEvent>(maxBatchSize);
        }

        public async Task Add(TEvent @event, bool skipped, CancellationToken token)
        {
            if (_target.Completion.IsFaulted)
                await _target.Completion;

            if (_target.Completion.IsCompleted)
                throw new InvalidOperationException("Target already completed.");

            await AddEvent(new BufferedEvent(@event, skipped), token);

            await CheckTimeout(token);
        }

        public Task CheckTimeout(CancellationToken token)
        {
            if (_events.Count == 0 || _timeout == Timeout.InfiniteTimeSpan)
                return Task.CompletedTask;

            var checkTimeoutStartTime = _toBeHandledEventsCount == 0
                ? _bufferStartTime
                : _batchStartTime;

            return checkTimeoutStartTime + _timeout <= DateTime.UtcNow
                ? TriggerSend(token)
                : Task.CompletedTask;
        }

        public async Task TriggerSend(CancellationToken token)
        {
            if (_events.Count == 0)
                return;

            var batch = new Batch(_events, _toBeHandledEventsCount);

            _events = new PooledList<BufferedEvent>(_maxBatchSize);
            _toBeHandledEventsCount = 0;

            if (!await _target.SendAsync(batch, token))
                throw new InvalidOperationException("Unable to send data to target");
        }

        private Task AddEvent(BufferedEvent @event, CancellationToken token)
        {
            if (_events.Count == 0)
                _bufferStartTime = DateTime.UtcNow;

            _events.Add(@event);

            if (!@event.Skipped)
            {
                if (_toBeHandledEventsCount == 0)
                    _batchStartTime = DateTime.UtcNow;

                ++_toBeHandledEventsCount;
            }

            if (_toBeHandledEventsCount >= _maxBatchSize || _events.Count >= _maxBufferSize)
                return TriggerSend(token);

            return Task.CompletedTask;
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

        [StructLayout(LayoutKind.Auto)]
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
    }
}