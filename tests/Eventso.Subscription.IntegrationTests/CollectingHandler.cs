namespace Eventso.Subscription.IntegrationTests;

public sealed class CollectingHandler : IMessageHandler<RedMessage>,
    IMessageHandler<GreenMessage>,
    IMessageHandler<BlueMessage>,
    IMessageHandler<BlackMessage>,
    IMessageHandler<IReadOnlyCollection<RedMessage>>,
    IMessageHandler<IReadOnlyCollection<GreenMessage>>,
    IMessageHandler<IReadOnlyCollection<BlueMessage>>,
    IMessageHandler<IReadOnlyCollection<BlackMessage>>
{
    private readonly Options _options;

    public CollectingHandler(Options options)
    {
        _options = options;
    }

    public WaitingList<RedMessage> Red { get; } = new();
    public WaitingList<GreenMessage> Green { get; } = new();
    public WaitingList<BlueMessage> Blue { get; } = new();
    public WaitingList<BlackMessage> Black { get; } = new();

    public Task Handle(RedMessage message, CancellationToken token)
    {
        Red.Add(message);
        return Task.Delay(_options.Delay, token);
    }

    public Task Handle(GreenMessage message, CancellationToken token)
    {
        Green.Add(message);
        return Task.Delay(_options.Delay, token);
    }

    public Task Handle(BlueMessage message, CancellationToken token)
    {
        Blue.Add(message);
        return Task.Delay(_options.Delay, token);
    }

    public Task Handle(BlackMessage message, CancellationToken token)
    {
        Black.Add(message);
        return Task.Delay(_options.Delay, token);
    }

    public Task Handle(IReadOnlyCollection<RedMessage> message, CancellationToken token)
    {
        Red.AddRange(message);
        return Task.Delay(_options.Delay, token);
    }

    public Task Handle(IReadOnlyCollection<GreenMessage> message, CancellationToken token)
    {
        Green.AddRange(message);
        return Task.Delay(_options.Delay, token);
    }

    public Task Handle(IReadOnlyCollection<BlueMessage> message, CancellationToken token)
    {
        Blue.AddRange(message);
        return Task.Delay(_options.Delay, token);
    }

    public Task Handle(IReadOnlyCollection<BlackMessage> message, CancellationToken token)
    {
        Black.AddRange(message);
        return Task.Delay(_options.Delay, token);
    }

    public record Options(TimeSpan Delay);
}