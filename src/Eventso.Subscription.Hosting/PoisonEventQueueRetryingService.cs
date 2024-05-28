using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Eventso.Subscription.Kafka;
using Eventso.Subscription.Kafka.DeadLetter;
using Eventso.Subscription.Observing.DeadLetter;

namespace Eventso.Subscription.Hosting;

public sealed class PoisonEventQueueRetryingService : IPoisonEventQueueRetryingService
{
    private readonly ILogger _logger;
    private readonly IReadOnlyCollection<Worker> _workers;

    public PoisonEventQueueRetryingService(
        IEnumerable<ISubscriptionCollection> subscriptions,
        IMessagePipelineFactory pipelineFactory,
        IMessageHandlersRegistry handlersRegistry,
        IPoisonEventQueueFactory poisonEventQueueFactory,
        IDeadLetterQueueScopeFactory deadLetterQueueScopeFactory,
        ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<SubscriptionHost>();

        _workers = subscriptions
            .SelectMany(x => x)
            .SelectMany(x => x.ClonePerConsumerInstance())
            .Select(x => CreateTopicRetryingService(
                x,
                pipelineFactory,
                handlersRegistry,
                poisonEventQueueFactory,
                deadLetterQueueScopeFactory,
                loggerFactory))
            .ToArray();
    }

    public async Task Run(CancellationToken token)
    {
        if (_workers.Count == 0)
            return;

        var workerTasks = _workers.Select(r => r.Run(token)).ToArray();
        await foreach (var _ in AsyncEnumerableEx.Merge(workerTasks).WithCancellation(token))
        {
            // do nothing
        }
    }

    private static Worker CreateTopicRetryingService(
        SubscriptionConfiguration config,
        IMessagePipelineFactory messagePipelineFactory,
        IMessageHandlersRegistry handlersRegistry,
        IPoisonEventQueueFactory poisonEventQueueFactory,
        IDeadLetterQueueScopeFactory deadLetterQueueScopeFactory,
        ILoggerFactory loggerFactory)
    {
        var valueDeserializer = new ValueDeserializer(
            new CompositeDeserializer(config.TopicConfigurations.Select(c => KeyValuePair.Create(c.Topic, c.Serializer))),
            handlersRegistry);
        var eventHandlers = config.TopicConfigurations
            .ToFrozenDictionary(
                c => c.Topic,
                // this event handler is crucial to work with both batch and single processing
                c => new Observing.EventHandler<Event>(
                    handlersRegistry,
                    messagePipelineFactory.Create(c.HandlerConfig)));

        var poisonEventQueue = poisonEventQueueFactory.Create(
            config.Settings.Config.GroupId,
            config.SubscriptionConfigurationId);
        var retryingService = new PoisonEventRetryingService(
            config.Settings.Config.GroupId,
            valueDeserializer,
            eventHandlers,
            deadLetterQueueScopeFactory,
            poisonEventQueue,
            loggerFactory.CreateLogger<PoisonEventRetryingService>());

        return new Worker(
            config.TopicConfigurations.Select(c => c.Topic).ToArray(),
            poisonEventQueue,
            retryingService,
            loggerFactory.CreateLogger<Worker>());
    }
    
    private sealed class Worker(
        string[] topics,
        IPoisonEventQueue poisonEventQueue,
        PoisonEventRetryingService poisonEventRetryingService,
        ILogger<Worker> logger)
    {
        public async IAsyncEnumerable<ConsumeResult<byte[], byte[]>> Run(
            [EnumeratorCancellation] CancellationToken token)
        {
            using var retryScope = logger.BeginScope(
                new[] { new KeyValuePair<string, string>("eventso_retry_topic", string.Join(",", topics)) });

            logger.LogInformation("Started event retrying");

            await foreach (var toRetry in poisonEventQueue.Peek(token))
            {
                await poisonEventRetryingService.Retry(toRetry, token);
                yield return toRetry;
            }

            logger.LogInformation("Finished event retrying");
        }
    }
}