namespace Eventso.Subscription.Tests;

public class IsServiceBasedMessageHandlersRegistryTest
{
    // non-significant "default" required to express "no handlers"
    [Fact]
    public void HandlerKindFlagsTest()
    {
        const HandlerKind defaultHandlerKind = default;

        Assert.NotEqual(HandlerKind.Single, defaultHandlerKind);
        Assert.NotEqual(HandlerKind.Batch, defaultHandlerKind);
        Assert.NotEqual(HandlerKind.Single | HandlerKind.Batch, defaultHandlerKind);
    }

    [Fact]
    public void ScanningWorks()
    {
        var sp = new ServiceCollection()
            .Scan(x => x.FromAssembliesOf(this.GetType())
                .AddClasses(tf => tf.AssignableTo(typeof(IMessageHandler<>)), publicOnly: false)
                .AsSelfWithInterfaces()
                .WithSingletonLifetime()
            )
            .AddSingleton<IMessageHandlersRegistry, IsServiceBasedMessageHandlersRegistry>()
            .BuildServiceProvider();

        var registry = sp.GetRequiredService<IMessageHandlersRegistry>();

        Assert.True(registry.ContainsHandlersFor(typeof(MsgA), out var msgAKind));
        Assert.Equal(HandlerKind.Single, msgAKind);

        Assert.True(registry.ContainsHandlersFor(typeof(MsgB), out var msgBKind));
        Assert.Equal(HandlerKind.Single | HandlerKind.Batch, msgBKind);

        Assert.False(registry.ContainsHandlersFor(typeof(MsgC), out var msgCKind));
        Assert.Equal(default(HandlerKind), msgCKind);
    }

    [Fact]
    public void OpenGenericWorks()
    {
        var sp = new ServiceCollection()
            .AddSingleton(typeof(IMessageHandler<>), typeof(GenericMsgHandler<>))
            .AddSingleton<IMessageHandlersRegistry, IsServiceBasedMessageHandlersRegistry>()
            .BuildServiceProvider();

        var registry = sp.GetRequiredService<IMessageHandlersRegistry>();

        Assert.True(registry.ContainsHandlersFor(typeof(MsgA), out var msgAKind));
        Assert.Equal(HandlerKind.Single | HandlerKind.Batch, msgAKind);

        Assert.True(registry.ContainsHandlersFor(typeof(MsgB), out var msgBKind));
        Assert.Equal(HandlerKind.Single | HandlerKind.Batch, msgBKind);

        Assert.True(registry.ContainsHandlersFor(typeof(MsgC), out var msgCKind));
        Assert.Equal(HandlerKind.Single | HandlerKind.Batch, msgCKind);

        Assert.True(registry.ContainsHandlersFor(typeof(object), out var objectKind));
        Assert.Equal(HandlerKind.Single | HandlerKind.Batch, objectKind);
    }
}

record class MsgA;
record class MsgB;
record class MsgC;

class MsgAHandler : IMessageHandler<MsgA>
{
    public Task Handle(MsgA message, CancellationToken token) => Task.CompletedTask;
}

class MsgBHandlerSingle : IMessageHandler<MsgB>
{
    public Task Handle(MsgB message, CancellationToken token) => Task.CompletedTask;
}

class MsgBHandlerBatch : IMessageHandler<IReadOnlyCollection<MsgB>>
{
    public Task Handle(IReadOnlyCollection<MsgB> message, CancellationToken token) => Task.CompletedTask;
}

class GenericMsgHandler<TMessage> : IMessageHandler<TMessage>
{
    public Task Handle(TMessage message, CancellationToken token) => Task.CompletedTask;
}
