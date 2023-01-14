namespace Eventso.Subscription.IntegrationTests;

public sealed class CollectingHandler : IMessageHandler<RedMessage>,
    IMessageHandler<GreenMessage>,
    IMessageHandler<BlueMessage>,
    IMessageHandler<IReadOnlyCollection<GreenMessage>>,
    IMessageHandler<IReadOnlyCollection<BlueMessage>>,
    IMessageHandler<IReadOnlyCollection<BlackMessage>>
{
    private readonly Options _options;

    public CollectingHandler(Options options)
    {
        _options = options;
    }

    public WaitingCollection<RedMessage> Red { get; } = new(new List<RedMessage>());
    public WaitingCollection<GreenMessage> Green { get; } = new(new List<GreenMessage>());
    public WaitingCollection<BlueMessage> Blue { get; } = new(new List<BlueMessage>());
    public WaitingCollection<BlackMessage> Black { get; } = new(new List<BlackMessage>());

    public WaitingCollection<RedMessage> RedSet { get; } = new(new HashSet<RedMessage>());
    public WaitingCollection<GreenMessage> GreenSet { get; } = new(new HashSet<GreenMessage>());
    public WaitingCollection<BlueMessage> BlueSet { get; } = new(new HashSet<BlueMessage>());
    public WaitingCollection<BlackMessage> BlackSet { get; } = new(new HashSet<BlackMessage>());


    public Task Handle(RedMessage message, CancellationToken token)
    {
        Red.Add(message);
        RedSet.Add(message);
        return Task.Delay(_options.Delay, token);
    }

    public Task Handle(GreenMessage message, CancellationToken token)
    {
        Green.Add(message);
        GreenSet.Add(message);
        return Task.Delay(_options.Delay, token);
    }

    public Task Handle(BlueMessage message, CancellationToken token)
    {
        Blue.Add(message);
        BlueSet.Add(message);
        return Task.Delay(_options.Delay, token);
    }

    public Task Handle(IReadOnlyCollection<GreenMessage> message, CancellationToken token)
    {
        Green.AddRange(message);
        GreenSet.AddRange(message);
        return Task.Delay(_options.Delay, token);
    }

    public Task Handle(IReadOnlyCollection<BlueMessage> message, CancellationToken token)
    {
        Blue.AddRange(message);
        BlueSet.AddRange(message);
        return Task.Delay(_options.Delay, token);
    }

    public Task Handle(IReadOnlyCollection<BlackMessage> message, CancellationToken token)
    {
        Black.AddRange(message);
        BlackSet.AddRange(message);
        return Task.Delay(_options.Delay, token);
    }

    public record Options(TimeSpan Delay);
}