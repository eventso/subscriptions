using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Eventso.Subscription.Observing.DeadLetter
{
    public sealed class AsyncLocalDeadLetterWatcher : IDeadLetterQueueScopeFactory, IDeadLetterQueue
    {
        public static readonly AsyncLocalDeadLetterWatcher Instance = new();

        private readonly AsyncLocal<IDeadLetterQueue> _localContext = new();

        public IDeadLetterQueueScope<TEvent> Create<TEvent>(string topic, TEvent @event)
            where TEvent : IEvent
            => Create(new SingleEventContext<TEvent>(topic, @event));

        public IDeadLetterQueueScope<TEvent> Create<TEvent>(string topic, IReadOnlyCollection<TEvent> events)
            where TEvent : IEvent
            => Create(new BatchEventContext<TEvent>(topic, events));

        public void Enqueue(Subscription.DeadLetter message)
            => _localContext.Value?.Enqueue(message);

        public void EnqueueRange(IEnumerable<Subscription.DeadLetter> messages)
            => _localContext.Value?.EnqueueRange(messages);

        private IDeadLetterQueueScope<TEvent> Create<TEvent>(IEventContext<TEvent> eventContext) where TEvent : IEvent
        {
            var queue = new DeadLetterQueue();
            var scope = new ScopedDeadLetterQueue<TEvent>(
                queue,
                eventContext,
                () => _localContext.Value = null);
            _localContext.Value = queue;
            return scope;
        }

        private interface IEventContext<TEvent>
            where TEvent : IEvent
        {
            IReadOnlyCollection<PoisonEvent<TEvent>> GetPoisonEvents(
                IReadOnlyDictionary<object, IReadOnlySet<string>> deadLetters);
        }
        
        private sealed class SingleEventContext<TEvent> : IEventContext<TEvent>
            where TEvent : IEvent
        {
            private readonly string _topic;
            private readonly TEvent _event;

            public SingleEventContext(string topic, TEvent @event)
            {
                _event = @event;
                _topic = topic;
            }

            public IReadOnlyCollection<PoisonEvent<TEvent>> GetPoisonEvents(
                IReadOnlyDictionary<object, IReadOnlySet<string>> deadLetters)
            {
                if (deadLetters.Count == 0)
                    return Array.Empty<PoisonEvent<TEvent>>();

                var (message, reasons) = deadLetters.Single();
                if (!Subscription.DeadLetter.MessageComparer.Equals(message, _event.GetMessage()))
                    throw new ApplicationException(
                        "Expected 1 poison event, but found 0 (event message and dead message mismatch).");

                // occurs rare, assuming we can afford array with 1 element here :)
                return new[] { new PoisonEvent<TEvent>(_topic, _event, reasons) };
            }
        }
        
        private sealed class BatchEventContext<TEvent> : IEventContext<TEvent>
            where TEvent : IEvent
        {
            private readonly string _topic;
            private readonly IReadOnlyCollection<TEvent> _events;

            public BatchEventContext(string topic, IReadOnlyCollection<TEvent> events)
            {
                _topic = topic;
                _events = events;
            }

            public IReadOnlyCollection<PoisonEvent<TEvent>> GetPoisonEvents(
                IReadOnlyDictionary<object, IReadOnlySet<string>> deadLetters)
            {
                if (deadLetters.Count == 0)
                    return Array.Empty<PoisonEvent<TEvent>>();

                var poisonEvents = new List<PoisonEvent<TEvent>>(deadLetters.Count);
                foreach (var @event in _events)
                {
                    if (!deadLetters.TryGetValue(@event.GetMessage(), out var reasons))
                        continue;

                    poisonEvents.Add(new PoisonEvent<TEvent>(_topic, @event, reasons));
                }

                if (poisonEvents.Count != deadLetters.Count)
                    throw new ApplicationException(
                        $"Expected {deadLetters.Count} poison events, but found {poisonEvents.Count} (event message and dead message mismatch).");

                return poisonEvents;
            }
        }

        private sealed class ScopedDeadLetterQueue<TEvent> : IDeadLetterQueueScope<TEvent>
            where TEvent : IEvent
        {
            private readonly Action _cleanup;
            private readonly DeadLetterQueue _queue;
            private readonly IEventContext<TEvent> _eventContext;

            public ScopedDeadLetterQueue(
                DeadLetterQueue queue,
                IEventContext<TEvent> eventContext,
                Action cleanup)
            {
                _queue = queue;
                _eventContext = eventContext;
                _cleanup = cleanup;
            }

            public IReadOnlyCollection<PoisonEvent<TEvent>> GetPoisonEvents()
                => _eventContext.GetPoisonEvents(_queue.GetDeadLetters());

            public void Dispose()
                => _cleanup();
        }
    }
}