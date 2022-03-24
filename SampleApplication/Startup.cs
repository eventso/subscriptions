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
                        new JsonMessageDeserializer<Event>()),
                types => types.FromAssemblyOf<Startup>());

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

        public record Event(string @event, DateTime date, long id);

        public class EventHandler : IMessageHandler<Event>
        {
            public Task Handle(Event message, CancellationToken token)
            {
                Console.WriteLine($"Event received {message.@event}, {message.date}, {message.id}");
                return Task.CompletedTask;
            }
        }
    }
}