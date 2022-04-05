using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using Eventso.Subscription.Observing.Batch;
using FluentAssertions;
using NSubstitute;
using Xunit;

namespace Eventso.Subscription.Tests
{
    public sealed class EventHandlerTests
    {
        private readonly List<object> _handledEvents = new();
        private readonly List<IReadOnlyCollection<object>> _handledBatches = new();
        private readonly Observing.EventHandler<TestEvent> _handler;
        private readonly Fixture _fixture = new();

        public EventHandlerTests()
        {
            var registry = Substitute.For<IMessageHandlersRegistry>();
            registry.ContainsHandlersFor(Arg.Any<Type>(), out Arg.Any<HandlerKind>())
                .Returns(x => { 
                    x[1] = HandlerKind.Batch;
                    return true;
                });

            var action = Substitute.For<IMessagePipelineAction>();
            action.Invoke(default(IReadOnlyCollection<RedMessage>), default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c =>
                {
                    _handledEvents.AddRange(c.Arg<IReadOnlyCollection<RedMessage>>());
                    _handledBatches.Add(c.Arg<IReadOnlyCollection<RedMessage>>());
                });

            _handler = new Observing.EventHandler<TestEvent>(registry, action);
        }

        [Fact]
        public async Task SingleTypeMessages_SameOrder()
        {
            var events = _fixture.CreateMany<(RedMessage e, Guid k)>(10)
                .Select(x => new TestEvent(x.k, x.e))
                .ToConvertibleCollection();

            await _handler.Handle(events, CancellationToken.None);

            _handledEvents.Should().BeEquivalentTo(
                events.Select(x => x.GetMessage()),
                c => c.WithStrictOrdering());

            _handledBatches.Should().HaveCount(1);
            _handledBatches.SelectMany(x => x).Should().HaveSameCount(events);
        }

        [Fact]
        public async Task EmptyMessages_NoBatches()
        {
            await _handler.Handle(Array.Empty<TestEvent>().ToConvertibleCollection(),
                CancellationToken.None);

            _handledEvents.Should().BeEmpty();

            _handledBatches.Should().HaveCount(0);
        }

        [Fact]
        public async Task MultiTypeMessages_Throws()
        {
            var events = _fixture.CreateMany<(RedMessage e, Guid k)>(10)
                .Select(x => new TestEvent(x.k, x.e))
                .Concat(_fixture.CreateMany<(BlueMessage e, Guid k)>(10)
                    .Select(x => new TestEvent(x.k, x.e)))
                .ToConvertibleCollection();

            Func<Task> act = () => _handler.Handle(events, CancellationToken.None);

            await act.Should().ThrowAsync<Exception>();
        }
    }
}