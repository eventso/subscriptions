using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Eventso.Subscription;
using Eventso.Subscription.Hosting;
using Eventso.Subscription.Kafka;
using Eventso.Subscription.Kafka.Insights;
using Eventso.Subscription.SpanJson;

namespace SampleApplication
{
    public class Startup
    {
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddSubscriptions(
                (subs, _) =>
                    subs.Add(
                        new ConsumerSettings(
                            "kafka:9092",
                            "test-group-id",
                            autoOffsetReset: AutoOffsetReset.Latest)
                        {
                            Topic = "some-topic"
                        },
                        new JsonMessageDeserializer<Message>()),
                types => types.FromAssemblyOf<Startup>());

            // services.AddKafkaListener(
            //     kafka => kafka
            //         .AddSubscriptions((subs, _) =>
            //             subs.Add(
            //                 new ConsumerSettings("kafka:9092", "test-group-id", autoOffsetReset: AutoOffsetReset.Latest)
            //                 {
            //                     Topic = "some-topic"
            //                 },
            //                 new JsonMessageDeserializer<Message>()))
            //         .ConfigureHandlersSelector(types => types.FromAssemblyOf<Startup>())
            //         .ConfigureDeadLetterQueue(e => e.UsePostgresStore(default(IConnectionFactory))));
            
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

        public record Message(string message, DateTime date, long id);

        public class MessageHandler : IMessageHandler<Message>
        {
            public Task Handle(Message message, CancellationToken token)
            {
                Console.WriteLine($"Message received {message.message}, {message.date}, {message.id}");
                return Task.CompletedTask;
            }
        }
    }
}