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
    public sealed class OrderedWithinKeyBatchHandlerTests
    {
        private readonly List<object> _handledEvents = new();
        private readonly List<IReadOnlyCollection<object>> _handledBatches = new();
        private readonly OrderedWithinKeyBatchHandler<TestMessage> _handler;
        private readonly Fixture _fixture = new();

        public OrderedWithinKeyBatchHandlerTests()
        {
            var action = Substitute.For<IMessageBatchPipelineAction>();
            action.Invoke<RedEvent>(default, default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c =>
                {
                    _handledEvents.AddRange(c.Arg<IReadOnlyCollection<RedEvent>>());
                    _handledBatches.Add(c.Arg<IReadOnlyCollection<RedEvent>>());
                });

            action.Invoke<BlueEvent>(default, default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c =>
                {
                    _handledEvents.AddRange(c.Arg<IReadOnlyCollection<BlueEvent>>());
                    _handledBatches.Add(c.Arg<IReadOnlyCollection<BlueEvent>>());
                });

            action.Invoke<GreenEvent>(default, default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c =>
                {
                    _handledEvents.AddRange(c.Arg<IReadOnlyCollection<GreenEvent>>());
                    _handledBatches.Add(c.Arg<IReadOnlyCollection<GreenEvent>>());
                });

            _handler = new OrderedWithinKeyBatchHandler<TestMessage>(action);
        }

        [Fact]
        public async Task SingleTypeMessages_SameOrder()
        {
            var events = _fixture.CreateMany<(RedEvent e, Guid k)>(10)
                .Select(x => new TestMessage(x.k, x.e))
                .ToConvertibleCollection();

            await _handler.Handle(events, CancellationToken.None);

            _handledEvents.Should().BeEquivalentTo(
                events.Select(x => x.GetPayload()),
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
                Create<RedEvent>(k, 0),
                Create<BlueEvent>(k, 1),
                Create<GreenEvent>(k, 2)
            }).ToConvertibleCollection();

            await _handler.Handle(events, CancellationToken.None);

            _handledEvents.Should().BeEquivalentTo(
                events.OrderBy(x => x.BatchNumber)
                    .ThenBy(x => x.GetKey())
                    .Select(x => x.GetPayload()),
                c => c.WithStrictOrdering());

            _handledBatches.Should().HaveCount(3);
            _handledBatches.SelectMany(x => x).Should().HaveSameCount(events);

            _handledBatches[0].Should().AllBeOfType<RedEvent>();
            _handledBatches[1].Should().AllBeOfType<BlueEvent>();
            _handledBatches[2].Should().AllBeOfType<GreenEvent>();
        }

        [Fact]
        public async Task StreamsContainsMessagesSameOrderByTypeNonSequentialByKey_OrderedByType()
        {
            var keys = _fixture.CreateMany<Guid>(3)
                .OrderBy(x => x)
                .ToArray();

            var events = new[]
            {
                Create<RedEvent>(keys[0], 0),
                Create<BlueEvent>(keys[0], 1),
                Create<RedEvent>(keys[1], 0),
                Create<GreenEvent>(keys[0], 2),
                Create<RedEvent>(keys[2], 0),
                Create<BlueEvent>(keys[1], 1),
                Create<BlueEvent>(keys[2], 1),
                Create<GreenEvent>(keys[2], 2),
                Create<GreenEvent>(keys[1], 2)
            }.ToConvertibleCollection();

            await _handler.Handle(events, CancellationToken.None);

            _handledEvents.Should().BeEquivalentTo(
                events.OrderBy(x => x.BatchNumber)
                    .ThenBy(x => x.GetKey())
                    .Select(x => x.GetPayload()),
                c => c.WithStrictOrdering());

            _handledBatches.Should().HaveCount(3);
            _handledBatches.SelectMany(x => x).Should().HaveSameCount(events);

            _handledBatches[0].Should().AllBeOfType<RedEvent>();
            _handledBatches[1].Should().AllBeOfType<BlueEvent>();
            _handledBatches[2].Should().AllBeOfType<GreenEvent>();

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
                Create<RedEvent>(keys[0], 0),
                Create<RedEvent>(keys[1], 0),
                Create<BlueEvent>(keys[1], 2),
                Create<RedEvent>(keys[0], 0),
                Create<RedEvent>(keys[1], 4),
                Create<GreenEvent>(keys[0], 1),
                Create<BlueEvent>(keys[1], 5),
                Create<RedEvent>(keys[2], 0),
                Create<BlueEvent>(keys[0], 2),
                Create<RedEvent>(keys[2], 0),
                Create<GreenEvent>(keys[2], 1),
                Create<GreenEvent>(keys[1], 6),
                Create<RedEvent>(keys[2], 4),
                Create<BlueEvent>(keys[0], 2),
                Create<GreenEvent>(keys[0], 3),
            }.ToConvertibleCollection();

            await _handler.Handle(events, CancellationToken.None);

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
                Create<GreenEvent>(keys[0], 0),
                Create<RedEvent>(keys[1], 3),
                Create<BlueEvent>(keys[2], 1),
                Create<BlueEvent>(keys[0], 1),
                Create<RedEvent>(keys[1], 3),
                Create<GreenEvent>(keys[2], 2),
                Create<GreenEvent>(keys[0], 2),
                Create<RedEvent>(keys[1], 3),
                Create<BlueEvent>(keys[2], 4),
                Create<GreenEvent>(keys[2], 5),
                Create<RedEvent>(keys[0], 3),
                Create<BlueEvent>(keys[1], 4),
            }.ToConvertibleCollection();


            await _handler.Handle(events, CancellationToken.None);

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

            var events = _fixture.CreateMany<RedEvent>(30)
                .Cast<object>()
                .Concat(_fixture.CreateMany<BlueEvent>(30))
                .Concat(_fixture.CreateMany<GreenEvent>(40))
                .Select((e, i) => new TestMessage(keys[i % keys.Length], e))
                .OrderBy(_ => Guid.NewGuid())
                .ToConvertibleCollection();

            await _handler.Handle(events, CancellationToken.None);

            var lookup = _handledEvents
                .Select(ev => (ev, events.Single(e => ReferenceEquals(e.GetPayload(), ev))))
                .ToLookup(x => x.Item2.GetKey(), x => x.ev);

            foreach (var key in keys)
            {
                lookup[key].Should().BeEquivalentTo(
                    events.Where(e => e.GetKey() == key)
                        .Select(x => x.GetPayload()),
                    c => c.WithStrictOrdering()
                );
            }
        }

        private void Assert(IEnumerable<TestMessage> events, Guid[] keys)
        {
            var lookup = _handledEvents
                .Select(ev => (ev, events.Single(e => ReferenceEquals(e.GetPayload(), ev))))
                .ToLookup(x => x.Item2.GetKey(), x => x.ev);

            foreach (var key in keys)
            {
                lookup[key].Should().BeEquivalentTo(
                    events.Where(e => e.GetKey() == key)
                        .Select(x => x.GetPayload()),
                    c => c.WithStrictOrdering()
                );
            }

            for (var i = 0; i < _handledBatches.Count; i++)
            {
                _handledBatches[i].Should()
                    .BeEquivalentTo(
                        events.Where(e => e.BatchNumber == i)
                            .OrderBy(x => x.GetKey())
                            .Select(x => x.GetPayload()),
                        c => c.WithStrictOrdering());
            }
        }

        private TestMessage Create<T>(Guid key, int batchNumber = 0) =>
            new TestMessage(key, _fixture.Create<T>(), batchNumber);
    }
}