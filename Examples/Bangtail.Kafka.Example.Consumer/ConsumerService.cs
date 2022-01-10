using Bangtail.Kafka.Helpers.AspNetCore;
using Bangtail.Kafka.Helpers.AspNetCore.Services;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Bangtail.Kafka.Example.Consumer
{
  public class ConsumerService : ConsumerBackgroundService
  {
    private readonly ILogger _logger;

    public ConsumerService(IConfigurationService configurationService, ILogger<ConsumerService> logger) : base(configurationService, logger)
    {
      _logger = logger;
    }

    protected async override Task ExecuteAsync(CancellationToken stoppingToken)
    {
      await Task.Yield();
      using (var consumer = new ConsumerBuilder<string, string>(ConsumerConfig).Build())
      {
        try
        {
          _logger.LogInformation($"Subscribing to topic(s) {string.Join(',', TopicsToSubscribe)}");
          consumer.Subscribe(TopicsToSubscribe);
          try
          {
            while (!stoppingToken.IsCancellationRequested)
            {
              var consumeResultBatch = ConsumeBatch(consumer);
              if (consumeResultBatch.Any())
              {
                var result = await ProcessBatchAsync(consumeResultBatch, handleEventAsync, stoppingToken);
                if (result != null)
                {
                  consumer.Commit(result);
                }
              }
            }
          }
          catch (ConsumeException ex)
          {
            _logger.LogError($"Error occured: {ex.Error.Reason}");
            consumer.Close();
          }
          return;
        }
        catch (OperationCanceledException)
        {
          _logger.LogError("OPERATION CANCELLED...");
          consumer.Close();
          return;
        }
      }
    }

    protected Func<string, CancellationToken, Task> handleEventAsync = (eventValue, cancellationToken) =>
    {
      Console.WriteLine($"Message received, value : {eventValue}");
      return Task.FromResult(true);
    };
  }
}
