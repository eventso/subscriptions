using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Eventso.Subscription.Kafka;
using Eventso.Subscription.Kafka.DeadLetter;
using Eventso.Subscription.Kafka.DeadLetter.Store;
using Eventso.Subscription.Observing.DeadLetter;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Hosting
{
    public sealed class SubscriptionHost : BackgroundService
    {
        private readonly IReadOnlyCollection<SubscriptionConfiguration> _subscriptions;
        private readonly IMessagePipelineFactory _messagePipelineFactory;
        private readonly IMessageHandlersRegistry _handlersRegistry;
        private readonly DeadLetterQueueOptions _deadLetterQueueOptions;
        private readonly IPoisonEventStore _poisonEventStore;
        private readonly IDeadLetterQueueScopeFactory _deadLetterQueueScopeFactory;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger _logger;

        public SubscriptionHost(
            IEnumerable<ISubscriptionCollection> subscriptions,
            IMessagePipelineFactory messagePipelineFactory,
            IMessageHandlersRegistry handlersRegistry,
            DeadLetterQueueOptions deadLetterQueueOptions,
            IPoisonEventStore poisonEventStore,
            IDeadLetterQueueScopeFactory deadLetterQueueScopeFactory,
            ILoggerFactory loggerFactory)
        {
            _subscriptions = (subscriptions ?? throw new ArgumentNullException(nameof(subscriptions)))
                .SelectMany(x => x)
                .ToArray();

            _messagePipelineFactory =
                messagePipelineFactory ?? throw new ArgumentNullException(nameof(messagePipelineFactory));
            _handlersRegistry = handlersRegistry;
            _deadLetterQueueOptions = deadLetterQueueOptions;
            _poisonEventStore = poisonEventStore;
            _deadLetterQueueScopeFactory = deadLetterQueueScopeFactory;
            _loggerFactory = loggerFactory;
            _logger = loggerFactory.CreateLogger<SubscriptionHost>();
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var subscriptionTasks = _subscriptions
                .SelectMany(c =>
                    Enumerable.Range(0, c.ConsumerInstances)
                        .Select(_ => RunConsuming(c, stoppingToken)))
                .ToArray();

            return Task.WhenAll(subscriptionTasks);
        }

        private async Task RunConsuming(SubscriptionConfiguration config, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    $"Subscription starting. Topic {config.Settings.Topic}. Group {config.Settings.Config.GroupId}");

                try
                {
                    using var consumer = CreateConsumer(config);
                    try
                    {
                        await consumer.Consume(cancellationToken);
                    }
                    finally
                    {
                        consumer.Close();
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation($"Subscription stopped. Topic: {config.Settings.Topic}.");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Subscription failed. Topic: {config.Settings.Topic}.");
                }
            }
        }

        private KafkaConsumer CreateConsumer(SubscriptionConfiguration config)
        {
            var poisonEventInbox = config.EnableDeadLetterQueue
                ? new PoisonEventInbox(
                    _poisonEventStore,
                    _deadLetterQueueOptions.MaxTopicQueueSize,
                    config.Settings,
                    _loggerFactory.CreateLogger<PoisonEventInbox>())
                : null;
            return new KafkaConsumer(
                new ObserverFactory(
                    config,
                    _messagePipelineFactory,
                    _handlersRegistry,
                    poisonEventInbox,
                    _deadLetterQueueScopeFactory,
                    _loggerFactory),
                new ValueObjectDeserializer(
                    config.Serializer,
                    _handlersRegistry),
                poisonEventInbox,
                config.Settings,
                _loggerFactory.CreateLogger<KafkaConsumer>());
        }
    }
}