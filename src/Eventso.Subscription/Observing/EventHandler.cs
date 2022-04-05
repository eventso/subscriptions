using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing
{
    public sealed class EventHandler<TEvent> : IEventHandler<TEvent>
        where TEvent : IEvent
    {
        private readonly IMessageHandlersRegistry _handlersRegistry;
        private readonly IMessagePipelineAction _pipelineAction;

        public EventHandler(
            IMessageHandlersRegistry handlersRegistry,
            IMessagePipelineAction pipelineAction)
        {
            _handlersRegistry = handlersRegistry;
            _pipelineAction = pipelineAction;
        }
        
        public Task Handle(TEvent @event, CancellationToken cancellationToken)
        {
            dynamic message = @event.GetMessage();
            return _pipelineAction.Invoke(
                message,
                cancellationToken);
        }

        public Task Handle(IConvertibleCollection<TEvent> events, CancellationToken cancellationToken)
        {
            if (events.Count == 0)
                return Task.CompletedTask;

            return HandleTyped(
                (dynamic)events[0].GetMessage(),
                events,
                cancellationToken);
        }

        private async Task HandleTyped<TMessage>(
            TMessage sample,
            IConvertibleCollection<TEvent> events,
            CancellationToken token)
            where TMessage : class
        {
            if (!_handlersRegistry.ContainsHandlersFor(typeof(TMessage), out var kind))
                return;

            if ((kind & HandlerKind.Batch) != 0)
                await _pipelineAction.Invoke(events.Convert(m => (TMessage)m.GetMessage()), token);

            if((kind & HandlerKind.Single) != 0)
                foreach (var @event in events)
                    await _pipelineAction.Invoke((TMessage)@event.GetMessage(), token);
        }
    }
}