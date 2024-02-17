namespace Eventso.Subscription.Observing;

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

    public async Task Handle(TEvent @event, CancellationToken cancellationToken)
    {
        using var activity = Diagnostic.ActivitySource.StartActivity(Diagnostic.EventHandlerHandle)?
            .AddTag("type", @event.GetMessage().GetType())
            .AddTag("count", 1);

        dynamic message = @event.GetMessage();

        using var tl = new TimeoutLogger("Handle Event", 15);
        try
        {
            await _pipelineAction.Invoke(message, cancellationToken);
        }
        catch (Exception exception)
        {
            activity?.SetException(exception);
            throw;
        }
    }

    public async Task Handle(IConvertibleCollection<TEvent> events, CancellationToken cancellationToken)
    {
        if (events.Count == 0)
            return;

        var firstMessage = events[0].GetMessage();

        using var activity = Diagnostic.ActivitySource.StartActivity(Diagnostic.EventHandlerHandle)?
            .AddTag("type", firstMessage.GetType())
            .AddTag("count", events.Count);

        using var tl = new TimeoutLogger("Handle Events", 15);
        try
        {
            await HandleTyped((dynamic)firstMessage, events, cancellationToken);
        }
        catch (Exception exception)
        {
            activity?.SetException(exception);
            throw;
        }
    }

    private async Task HandleTyped<TMessage>(
        TMessage _,
        IConvertibleCollection<TEvent> events,
        CancellationToken token)
        where TMessage : class
    {
        if (!_handlersRegistry.ContainsHandlersFor(typeof(TMessage), out var kind))
            return;

        if ((kind & HandlerKind.Batch) != 0)
            await _pipelineAction.Invoke(events.Convert(m => (TMessage)m.GetMessage()), token);

        if ((kind & HandlerKind.Single) != 0)
            foreach (var @event in events)
                await _pipelineAction.Invoke((TMessage)@event.GetMessage(), token);
    }
}