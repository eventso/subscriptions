using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Eventso.Subscription.Configurations;
using Eventso.Subscription.Hosting;
using Eventso.Subscription.Kafka;
using Eventso.Subscription.Kafka.DeadLetter.Postgres;
using Eventso.Subscription.Kafka.Insights;
using Eventso.Subscription.SpanJson;
using Npgsql;
using Polly;

namespace SampleApplication;

public class Startup
{
    public void ConfigureServices(IServiceCollection services)
    {
        // services.AddSubscriptions(
        //     (subs, _) =>
        //         subs.MarkPoisoned(
        //             new ConsumerSettings(
        //                 "kafka:9092",
        //                 "test-group-id",
        //                 autoOffsetReset: AutoOffsetReset.Latest)
        //             {
        //                 Topic = "some-topic"
        //             },
        //             new JsonMessageDeserializer<Message>()),
        //     types => types.FromAssemblyOf<Startup>());
        //
        // services.AddMvc()
        //     .AddKafkaInsights();
        //
        // services.AddSwaggerGen();


        var brokers = "localhost:9092";
        var groupId = "sample-app";
        var enableDlq = true;

        CreateTopics(brokers);

        AddSubscriptions(services, brokers, groupId, enableDlq);

        services.AddSingleton(CreateProducer(brokers));

        services.AddMvc()
            .AddKafkaInsights();

        services.AddSwaggerGen();
    }

    public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
    {
        if (env.IsDevelopment())
        {
            app.UseDeveloperExceptionPage();
        }

        app.UseRouting();

        app.UseEndpoints(endpoints =>
        {
            endpoints.MapControllers();
            endpoints.MapSwagger();
        });

        app.UseSwagger();
        app.UseSwaggerUI();
    }

    private static void CreateTopics(string brokers)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = brokers }).Build();
        try
        {
            adminClient.CreateTopicsAsync(
                    new[]
                    {
                        new TopicSpecification { Name = "no-error-single", ReplicationFactor = 1, NumPartitions = 1 },
                        new TopicSpecification { Name = "exception-single", ReplicationFactor = 1, NumPartitions = 1 },
                        new TopicSpecification { Name = "poison-single", ReplicationFactor = 1, NumPartitions = 1 },
                        new TopicSpecification { Name = "no-error-batch", ReplicationFactor = 1, NumPartitions = 1 },
                        new TopicSpecification { Name = "exception-batch", ReplicationFactor = 1, NumPartitions = 1 },
                        new TopicSpecification { Name = "poison-batch", ReplicationFactor = 1, NumPartitions = 1 },
                    },
                    options: new CreateTopicsOptions()
                    {
                        
                    })
                .GetAwaiter()
                .GetResult();
        }
        catch (CreateTopicsException e)
        {
            Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
        }
    }

    private IProducer<byte[], string> CreateProducer(string brokers)
    {
        return new ProducerBuilder<byte[], string>(
                new ProducerConfig()
                {
                    BootstrapServers = brokers,
                    Acks = Acks.All,
                    RequestTimeoutMs = 3 * 60 * 1000,
                    LingerMs = 300,
                    EnableIdempotence = true,
                    MaxInFlight = 1
                })
            .Build();
    }

    private static void AddSubscriptions(IServiceCollection services, string brokers, string groupId, bool enableDlq)
    {
        services.AddSubscriptions(
            (subs, _) =>
            {
                Add<NoErrorSingleMessageHandler.NoErrorSingleMessage>(NoErrorSingleMessageHandler.Topic);
                Add<ExceptionSingleMessageHandler.ExceptionSingleMessage>(ExceptionSingleMessageHandler.Topic);
                Add<PoisonSingleMessageHandler.PoisonSingleMessage>(PoisonSingleMessageHandler.Topic);
                AddBatch<NoErrorBatchMessageHandler.NoErrorBatchMessage>(NoErrorBatchMessageHandler.Topic);
                AddBatch<ExceptionBatchMessageHandler.ExceptionBatchMessage>(ExceptionBatchMessageHandler.Topic);
                AddBatch<PoisonBatchMessageHandler.PoisonBatchMessage>(PoisonBatchMessageHandler.Topic);
                void Add<T>(string topic)
                    => subs.Add(new ConsumerSettings(brokers, groupId, autoOffsetReset: AutoOffsetReset.Latest)
                        {
                            Topic = topic
                        },
                        new JsonMessageDeserializer<T>(),
                        new HandlerConfiguration
                        {
                            ResiliencePipeline = ResiliencePipeline.Empty
                        });

                void AddBatch<T>(string topic)
                    => subs.AddBatch(new ConsumerSettings(brokers, groupId, autoOffsetReset: AutoOffsetReset.Latest)
                        {
                            Topic = topic
                        },
                        new BatchConfiguration() { MaxBatchSize = 3, MaxBufferSize = 5 },
                        new JsonMessageDeserializer<T>(),
                        new HandlerConfiguration
                        {
                            ResiliencePipeline = ResiliencePipeline.Empty
                        });
            },
            types => types.FromCallingAssembly());

        if (enableDlq)
            services.AddPostgresDeadLetterQueue<ConnectionFactory>(installSchema: true);
    }

    private sealed class ConnectionFactory : IConnectionFactory
    {
        public NpgsqlConnection ReadWrite()
            => new("Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=postgres;");

        public NpgsqlConnection ReadOnly()
            => ReadWrite();
    }
}