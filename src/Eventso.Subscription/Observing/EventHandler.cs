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

    public async Task Handle(TEvent @event, HandlingContext context, CancellationToken cancellationToken)
    {
        using var activity = Diagnostic.ActivitySource.StartActivity(Diagnostic.EventHandlerHandle)?
            .AddTag("type", @event.GetMessage().GetType())
            .AddTag("count", 1);

        dynamic message = @event.GetMessage();

        try
        {
            await _pipelineAction.Invoke(message, context, cancellationToken);
        }
        catch (Exception exception)
        {
            activity?.SetException(exception);
            throw;
        }
    }

    public async Task Handle(IConvertibleCollection<TEvent> events, HandlingContext context, CancellationToken cancellationToken)
    {
        if (events.Count == 0)
            return;

        var firstMessage = events[0].GetMessage();

        using var activity = Diagnostic.ActivitySource.StartActivity(Diagnostic.EventHandlerHandle)?
            .AddTag("type", firstMessage.GetType())
            .AddTag("count", events.Count);

        try
        {
            await HandleTyped((dynamic)firstMessage, events, context, cancellationToken);
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
        HandlingContext context,
        CancellationToken token)
        where TMessage : class
    {
        if (!_handlersRegistry.ContainsHandlersFor(typeof(TMessage), out var kind))
            return;

        if ((kind & HandlerKind.Batch) != 0)
            await _pipelineAction.Invoke(events.Convert(m => (TMessage)m.GetMessage()), context, token);

        if ((kind & HandlerKind.Single) != 0)
            foreach (var @event in events)
                await _pipelineAction.Invoke((TMessage)@event.GetMessage(), context, token);
    }
}