namespace Eventso.Subscription.IntegrationTests;

public sealed record RedMessage(int Key, string Value, decimal Amount, bool Check, char Symbol)
    : IKeyedMessage;

public sealed record GreenMessage(int Key, int Id, string Name)
    : IKeyedMessage;

public sealed record BlueMessage(int Key, long Id, string Ticker, byte Index)
    : IKeyedMessage;

public sealed record BlackMessage(int Key, Guid Value)
    : IKeyedMessage;

public sealed record WrongBlackMessage(int Key, string Value)
    : IKeyedMessage;