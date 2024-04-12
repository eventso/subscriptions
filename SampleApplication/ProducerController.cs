using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

namespace SampleApplication;

[Route("producer")]
[ApiController]
public sealed class ProducerController : ControllerBase
{
    private readonly IProducer<byte[],string> _producer;

    public ProducerController(IProducer<byte[], string> producer)
        => _producer = producer;

    [HttpPost(NoErrorSingleMessageHandler.Topic)]
    public void Produce(NoErrorSingleMessageHandler.NoErrorSingleMessage message)
        => Produce(NoErrorSingleMessageHandler.Topic, message);

    [HttpPost(NoErrorBatchMessageHandler.Topic)]
    public void Produce(NoErrorBatchMessageHandler.NoErrorBatchMessage[] messages)
    {
        foreach (var message in messages)
            Produce(NoErrorBatchMessageHandler.Topic, message);
    }

    [HttpPost(ExceptionSingleMessageHandler.Topic)]
    public void Produce(ExceptionSingleMessageHandler.ExceptionSingleMessage message)
        => Produce(ExceptionSingleMessageHandler.Topic, message);

    [HttpPost(ExceptionBatchMessageHandler.Topic)]
    public void Produce(ExceptionBatchMessageHandler.ExceptionBatchMessage[] messages)
    {
        foreach (var message in messages)
            Produce(NoErrorBatchMessageHandler.Topic, message);
    }

    [HttpPost(PoisonSingleMessageHandler.Topic)]
    public void Produce(PoisonSingleMessageHandler.PoisonSingleMessage message)
        => Produce(PoisonSingleMessageHandler.Topic, message);

    [HttpPost(PoisonBatchMessageHandler.Topic)]
    public void Produce(PoisonBatchMessageHandler.PoisonBatchMessage[] messages)
    {
        foreach (var message in messages)
            Produce(NoErrorBatchMessageHandler.Topic, message);
    }

    private void Produce<T>(string topic, T sampleMessage)
        where T : ISampleMessage
    {
        var keySerialized = sampleMessage.Guid.ToByteArray();

        var kafkaMessage = new Message<byte[], string>()
        {
            Key = keySerialized,
            Value = JsonSerializer.Serialize(sampleMessage)
        };

        _producer.Produce(topic, kafkaMessage);
    }
}