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
    public sealed class OrderedWithinKeyEventHandlerTests
    {
        private const string Topic = "IDDQD"; 

        private readonly List<object> _handledEvents = new();
        private readonly List<IReadOnlyCollection<object>> _handledBatches = new();
        private readonly OrderedWithinKeyEventHandler<TestEvent> _handler;
        private readonly Fixture _fixture = new();

        public OrderedWithinKeyEventHandlerTests()
        {
            var registry = Substitute.For<IMessageHandlersRegistry>();
            registry
                .ContainsHandlersFor(Arg.Any<Type>(), out Arg.Any<HandlerKind>())
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

            action.Invoke(default(IReadOnlyCollection<BlueMessage>), default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c =>
                {
                    _handledEvents.AddRange(c.Arg<IReadOnlyCollection<BlueMessage>>());
                    _handledBatches.Add(c.Arg<IReadOnlyCollection<BlueMessage>>());
                });

            action.Invoke(default(IReadOnlyCollection<GreenMessage>), default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c =>
                {
                    _handledEvents.AddRange(c.Arg<IReadOnlyCollection<GreenMessage>>());
                    _handledBatches.Add(c.Arg<IReadOnlyCollection<GreenMessage>>());
                });

            var eventHandler = new Observing.EventHandler<TestEvent>(registry, action);
            _handler = new OrderedWithinKeyEventHandler<TestEvent>(eventHandler);
        }

        [Fact]
        public async Task SingleTypeMessages_SameOrder()
        {
            var events = _fixture.CreateMany<(RedMessage e, Guid k)>(10)
                .Select(x => new TestEvent(x.k, x.e))
                .ToConvertibleCollection();

            await _handler.Handle(Topic, events, CancellationToken.None);

            _handledEvents.Should().BeEquivalentTo(
                events.Select(x => x.GetMessage()),
                c => c.WithStrictOrdering());

            _handledBatches.Should().HaveCount(1);
            _handledBatches.SelectMany(x => x).Should().HaveSameCount(events);
        }

        [Fact]
        public async Task StreamsContainsMessagesSameOrderByType_OrderedByType()
        {
            var keys = _fixture.CreateMany<Guid>(3)
                .OrderBy(x => x);

            var events = keys.SelectMany(k => new[]
            {
                Create<RedMessage>(k, 0),
                Create<BlueMessage>(k, 1),
                Create<GreenMessage>(k, 2)
            }).ToConvertibleCollection();

            await _handler.Handle(Topic, events, CancellationToken.None);

            _handledEvents.Should().BeEquivalentTo(
                events.OrderBy(x => x.BatchNumber)
                    .ThenBy(x => x.GetKey())
                    .Select(x => x.GetMessage()),
                c => c.WithStrictOrdering());

            _handledBatches.Should().HaveCount(3);
            _handledBatches.SelectMany(x => x).Should().HaveSameCount(events);

            _handledBatches[0].Should().AllBeOfType<RedMessage>();
            _handledBatches[1].Should().AllBeOfType<BlueMessage>();
            _handledBatches[2].Should().AllBeOfType<GreenMessage>();
        }

        [Fact]
        public async Task StreamsContainsMessagesSameOrderByTypeNonSequentialByKey_OrderedByType()
        {
            var keys = _fixture.CreateMany<Guid>(3)
                .OrderBy(x => x)
                .ToArray();

            var events = new[]
            {
                Create<RedMessage>(keys[0], 0),
                Create<BlueMessage>(keys[0], 1),
                Create<RedMessage>(keys[1], 0),
                Create<GreenMessage>(keys[0], 2),
                Create<RedMessage>(keys[2], 0),
                Create<BlueMessage>(keys[1], 1),
                Create<BlueMessage>(keys[2], 1),
                Create<GreenMessage>(keys[2], 2),
                Create<GreenMessage>(keys[1], 2)
            }.ToConvertibleCollection();

            await _handler.Handle(Topic, events, CancellationToken.None);

            _handledEvents.Should().BeEquivalentTo(
                events.OrderBy(x => x.BatchNumber)
                    .ThenBy(x => x.GetKey())
                    .Select(x => x.GetMessage()),
                c => c.WithStrictOrdering());

            _handledBatches.Should().HaveCount(3);
            _handledBatches.SelectMany(x => x).Should().HaveSameCount(events);

            _handledBatches[0].Should().AllBeOfType<RedMessage>();
            _handledBatches[1].Should().AllBeOfType<BlueMessage>();
            _handledBatches[2].Should().AllBeOfType<GreenMessage>();

            Assert(events, keys);
        }

        [Fact]
        public async Task RandomOrder1_OrderedByType()
        {
            var keys = _fixture.CreateMany<Guid>(3)
                .OrderBy(x => x)
                .ToArray();

            var events = new[]
            {
                Create<RedMessage>(keys[0], 0),
                Create<RedMessage>(keys[1], 0),
                Create<BlueMessage>(keys[1], 2),
                Create<RedMessage>(keys[0], 0),
                Create<RedMessage>(keys[1], 4),
                Create<GreenMessage>(keys[0], 1),
                Create<BlueMessage>(keys[1], 5),
                Create<RedMessage>(keys[2], 0),
                Create<BlueMessage>(keys[0], 2),
                Create<RedMessage>(keys[2], 0),
                Create<GreenMessage>(keys[2], 1),
                Create<GreenMessage>(keys[1], 6),
                Create<RedMessage>(keys[2], 4),
                Create<BlueMessage>(keys[0], 2),
                Create<GreenMessage>(keys[0], 3),
            }.ToConvertibleCollection();

            await _handler.Handle(Topic, events, CancellationToken.None);

            Assert(events, keys);
        }

        [Fact]
        public async Task RandomOrder2_OrderedByType()
        {
            var keys = _fixture.CreateMany<Guid>(3)
                .OrderBy(x => x)
                .ToArray();

            var events = new[]
            {
                Create<GreenMessage>(keys[0], 0),
                Create<RedMessage>(keys[1], 3),
                Create<BlueMessage>(keys[2], 1),
                Create<BlueMessage>(keys[0], 1),
                Create<RedMessage>(keys[1], 3),
                Create<GreenMessage>(keys[2], 2),
                Create<GreenMessage>(keys[0], 2),
                Create<RedMessage>(keys[1], 3),
                Create<BlueMessage>(keys[2], 4),
                Create<GreenMessage>(keys[2], 5),
                Create<RedMessage>(keys[0], 3),
                Create<BlueMessage>(keys[1], 4),
            }.ToConvertibleCollection();


            await _handler.Handle(Topic, events, CancellationToken.None);

            Assert(events, keys);
        }


        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        [InlineData(5)]
        [InlineData(6)]
        [InlineData(7)]
        public async Task RandomOrder3_OrderedByType(int keysCount)
        {
            var keys = _fixture.CreateMany<Guid>(keysCount)
                .OrderBy(x => x)
                .ToArray();

            var events = _fixture.CreateMany<RedMessage>(30)
                .Cast<object>()
                .Concat(_fixture.CreateMany<BlueMessage>(30))
                .Concat(_fixture.CreateMany<GreenMessage>(40))
                .Select((e, i) => new TestEvent(keys[i % keys.Length], e))
                .OrderBy(_ => Guid.NewGuid())
                .ToConvertibleCollection();

            await _handler.Handle(Topic, events, CancellationToken.None);

            var lookup = _handledEvents
                .Select(ev => (ev, events.Single(e => ReferenceEquals(e.GetMessage(), ev))))
                .ToLookup(x => x.Item2.GetKey(), x => x.ev);

            foreach (var key in keys)
            {
                lookup[key].Should().BeEquivalentTo(
                    events.Where(e => e.GetKey() == key)
                        .Select(x => x.GetMessage()),
                    c => c.WithStrictOrdering()
                );
            }
        }

        private void Assert(IEnumerable<TestEvent> events, Guid[] keys)
        {
            var lookup = _handledEvents
                .Select(ev => (ev, events.Single(e => ReferenceEquals(e.GetMessage(), ev))))
                .ToLookup(x => x.Item2.GetKey(), x => x.ev);

            foreach (var key in keys)
            {
                lookup[key].Should().BeEquivalentTo(
                    events.Where(e => e.GetKey() == key)
                        .Select(x => x.GetMessage()),
                    c => c.WithStrictOrdering()
                );
            }

            for (var i = 0; i < _handledBatches.Count; i++)
            {
                _handledBatches[i].Should()
                    .BeEquivalentTo(
                        events.Where(e => e.BatchNumber == i)
                            .OrderBy(x => x.GetKey())
                            .Select(x => x.GetMessage()),
                        c => c.WithStrictOrdering());
            }
        }

        private TestEvent Create<T>(Guid key, int batchNumber = 0) =>
            new TestEvent(key, _fixture.Create<T>(), batchNumber);
    }
}