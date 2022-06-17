using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.Batch.ErrorHandling
{
    internal class BreakdownFailedBufferProcessor<TEvent> : IFailedBufferProcessor<TEvent>
        where TEvent : IEvent
    {
        private readonly IEventHandler<TEvent> _handler;
        private readonly IConsumer<TEvent> _consumer;

        public BreakdownFailedBufferProcessor(IEventHandler<TEvent> handler, IConsumer<TEvent> consumer)
        {
            _handler = handler;
            _consumer = consumer;
        }

        public async Task Process(
            IReadOnlyList<Buffer<TEvent>.BufferedEvent> bufferedEvents,
            CancellationToken cancellationToken)
        {
            SingleEventCollection convertibleCollection = null;

            for (var i = 0; i < bufferedEvents.Count; i++)
            {
                var bufferedEvent = bufferedEvents[i];
                var @event = bufferedEvent.Event;

                if (bufferedEvent.Skipped)
                {
                    _consumer.Acknowledge(@event);

                    continue;
                }

                if (convertibleCollection is null)
                    convertibleCollection = new SingleEventCollection(@event);
                else
                    convertibleCollection.SetEvent(@event);

                await _handler.Handle(convertibleCollection, cancellationToken);

                _consumer.Acknowledge(@event);
            }
        }

        private class SingleEventCollection : IConvertibleCollection<TEvent>
        {
            private TEvent _event;

            public SingleEventCollection(TEvent @event)
                => _event = @event;

            public void SetEvent(TEvent @event)
                => _event = @event;

            public IEnumerator<TEvent> GetEnumerator()
            {
                yield return _event;
            }

            IEnumerator IEnumerable.GetEnumerator()
                => GetEnumerator();

            public int Count => 1;

            public TEvent this[int index]
                => index == 0 ? _event : throw new IndexOutOfRangeException();

            public IReadOnlyCollection<TOut> Convert<TOut>(Converter<TEvent, TOut> converter)
                => new[] {converter(_event)};

            public bool OnlyContainsSame<TValue>(Func<TEvent, TValue> valueConverter)
                => true;
        }
    }
}