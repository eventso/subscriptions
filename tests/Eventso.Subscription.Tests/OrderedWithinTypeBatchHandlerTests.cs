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
        private readonly OrderedWithinTypeBatchHandler<TestEvent> _handler;
        private readonly Fixture _fixture = new();

        public OrderedWithinTypeBatchHandlerTests()
        {
            var action = Substitute.For<IMessageBatchPipelineAction>();
            action.Invoke<RedMessage>(default, default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c =>
                {
                    _handledEvents.AddRange(c.Arg<IReadOnlyCollection<RedMessage>>());
                    _handledBatches.Add(c.Arg<IReadOnlyCollection<RedMessage>>());
                });

            action.Invoke<BlueMessage>(default, default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c =>
                {
                    _handledEvents.AddRange(c.Arg<IReadOnlyCollection<BlueMessage>>());
                    _handledBatches.Add(c.Arg<IReadOnlyCollection<BlueMessage>>());
                });

            action.Invoke<GreenMessage>(default, default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c =>
                {
                    _handledEvents.AddRange(c.Arg<IReadOnlyCollection<GreenMessage>>());
                    _handledBatches.Add(c.Arg<IReadOnlyCollection<GreenMessage>>());
                });

            _handler = new OrderedWithinTypeBatchHandler<TestEvent>(action);
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
        public async Task RandomOrder_OrderedByType()
        {
            var keys = _fixture.CreateMany<Guid>(3)
                .OrderBy(x => x)
                .ToArray();

            var events = new[]
            {
                Create<RedMessage>(keys[0], 0),
                Create<RedMessage>(keys[1], 0),
                Create<BlueMessage>(keys[1], 1),
                Create<RedMessage>(keys[0], 0),
                Create<RedMessage>(keys[1], 0),
                Create<GreenMessage>(keys[0], 2),
                Create<BlueMessage>(keys[1], 1),
                Create<RedMessage>(keys[2], 0),
                Create<BlueMessage>(keys[0], 1),
                Create<RedMessage>(keys[2], 0),
                Create<GreenMessage>(keys[2], 2),
                Create<GreenMessage>(keys[1], 2),
                Create<RedMessage>(keys[2], 0),
                Create<BlueMessage>(keys[0], 1),
                Create<GreenMessage>(keys[0], 2),
            }.ToConvertibleCollection();

            await _handler.Handle(events, CancellationToken.None);

            for (var i = 0; i < _handledBatches.Count; i++)
            {
                _handledBatches[i].Should()
                    .BeEquivalentTo(
                        events.Where(e => e.BatchNumber == i)
                            .Select(x => x.GetMessage()),
                        c => c.WithStrictOrdering());
            }
        }

        private TestEvent Create<T>(Guid key, int batchNumber = 0) =>
            new TestEvent(key, _fixture.Create<T>(), batchNumber);
    }
}