namespace Eventso.Subscription.Pipeline;

public sealed class MessageHandlingAction : IMessagePipelineAction
{
    private readonly IMessageHandlerScopeFactory _scopeFactory;
    private readonly bool _executeInParallel;

    public MessageHandlingAction(IMessageHandlerScopeFactory scopeFactory, bool executeInParallel)
    {
        _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
        _executeInParallel = executeInParallel;
    }

    public async Task Invoke<T>(T message, CancellationToken token) where T : notnull
    {
        if (message == null) throw new ArgumentNullException(nameof(message));

        using var scope = _scopeFactory.BeginScope();

        var handlers = scope.Resolve<T>();

        if (_executeInParallel)
            await ExecuteInParallel(message, handlers, token);
        else
            await ExecuteSequentially(message, handlers, token);
    }

    private static async Task ExecuteSequentially<T>(
        T message,
        IEnumerable<IMessageHandler<T>> handlers,
        CancellationToken token)
    {
        foreach (var handler in handlers)
        {
            token.ThrowIfCancellationRequested();

            await Handle(handler, message, token);
        }
    }

    private static Task ExecuteInParallel<T>(
        T message,
        IEnumerable<IMessageHandler<T>> handlers,
        CancellationToken token)
    {
        var tasks = new List<Task>();

        foreach (var handler in handlers)
            tasks.Add(Handle(handler, message, token));

        return Task.WhenAll(tasks);
    }

    private static async Task Handle<T>(IMessageHandler<T> handler, T message, CancellationToken token)
    {
        //temp root
        using var activity = Diagnostic.StartRooted(Diagnostic.PipelineHandle).Activity?
            .AddTag("type", handler.GetType());

        try
        {
            await handler.Handle(message, token);
        }
        catch (Exception ex)
        {
            activity?.SetException(ex);
            throw;
        }
    }
}