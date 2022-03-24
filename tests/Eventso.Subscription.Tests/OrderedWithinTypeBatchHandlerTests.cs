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
    public sealed class OrderedWithinTypeBatchHandlerTests
    {
        private readonly List<object> _handledEvents = new();
        private readonly List<IReadOnlyCollection<object>> _handledBatches = new();
        private readonly OrderedWithinTypeBatchHandler<TestMessage> _handler;
        private readonly Fixture _fixture = new();

        public OrderedWithinTypeBatchHandlerTests()
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

            _handler = new OrderedWithinTypeBatchHandler<TestMessage>(action);
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
        public async Task RandomOrder_OrderedByType()
        {
            var keys = _fixture.CreateMany<Guid>(3)
                .OrderBy(x => x)
                .ToArray();

            var events = new[]
            {
                Create<RedEvent>(keys[0], 0),
                Create<RedEvent>(keys[1], 0),
                Create<BlueEvent>(keys[1], 1),
                Create<RedEvent>(keys[0], 0),
                Create<RedEvent>(keys[1], 0),
                Create<GreenEvent>(keys[0], 2),
                Create<BlueEvent>(keys[1], 1),
                Create<RedEvent>(keys[2], 0),
                Create<BlueEvent>(keys[0], 1),
                Create<RedEvent>(keys[2], 0),
                Create<GreenEvent>(keys[2], 2),
                Create<GreenEvent>(keys[1], 2),
                Create<RedEvent>(keys[2], 0),
                Create<BlueEvent>(keys[0], 1),
                Create<GreenEvent>(keys[0], 2),
            }.ToConvertibleCollection();

            await _handler.Handle(events, CancellationToken.None);

            for (var i = 0; i < _handledBatches.Count; i++)
            {
                _handledBatches[i].Should()
                    .BeEquivalentTo(
                        events.Where(e => e.BatchNumber == i)
                            .Select(x => x.GetPayload()),
                        c => c.WithStrictOrdering());
            }
        }

        private TestMessage Create<T>(Guid key, int batchNumber = 0) =>
            new TestMessage(key, _fixture.Create<T>(), batchNumber);
    }
}