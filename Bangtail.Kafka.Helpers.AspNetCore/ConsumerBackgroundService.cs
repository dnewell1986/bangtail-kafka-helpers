using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Bangtail.Kafka.Helpers.AspNetCore.Constants;
using Bangtail.Kafka.Helpers.AspNetCore.Exceptions;
using Bangtail.Kafka.Helpers.AspNetCore.Services;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Bangtail.Kafka.Helpers.AspNetCore
{
  /// <summary>
  /// Base class for implementing a long running Kafka Consumer Client.
  /// </summary>
  public abstract class ConsumerBackgroundService : IHostedService, IDisposable
  {
    private Task _executingTask;
    private readonly CancellationTokenSource _stoppingCts = new CancellationTokenSource();
    private readonly ILogger _logger;
    private const int DEFAULT_MAX_BATCH_SIZE = 50;

    protected readonly IConfiguration AppConfig;
    protected ConsumerConfig ConsumerConfig;
    protected int MaxBatchSize;
    /// <summary>
    /// Property that stores the list of Kakfa topics, from the configuration, to subscribe to
    /// </summary>
    protected List<string> TopicsToSubscribe = new List<string>();

    public ConsumerBackgroundService(IConfigurationService configurationService, ILogger logger)
    {
      AppConfig = configurationService.GetConfiguration();
      ConsumerConfig = configurationService.GetConsumerConfig(AppConfig);
      _logger = logger;

      MaxBatchSize = GetBatchSize();
      SetupKafkaTopics();
    }

    /// <summary>
    /// This method is called when the IHostedService starts. The implementation should return a task that represents
    /// the lifetime of the long running operation(s) being performed.
    /// </summary>
    /// <param name="stoppingToken">Triggered when IHostedService.StopAsync(CancellationToken) is called.</param>
    /// <returns>A Task that represents the long running operations.</returns>
    protected abstract Task ExecuteAsync(CancellationToken stoppingToken);

    /// <summary>
    /// Triggered when the application host is ready to start the service.
    /// </summary>
    /// <param name="cancellationToken">Indicates that the start process has been aborted.</param>
    public virtual Task StartAsync(CancellationToken cancellationToken)
    {
      _executingTask = ExecuteAsync(_stoppingCts.Token);

      if (_executingTask.IsCompleted)
      {
        return _executingTask;
      }

      return Task.CompletedTask;
    }

    /// <summary>
    /// Triggered when the application host is performing a graceful shutdown.
    /// </summary>
    /// <param name="cancellationToken">Indicates that the shutdown process should no longer be graceful.</param>
    public virtual async Task StopAsync(CancellationToken cancellationToken)
    {
      if (_executingTask == null)
      {
        return;
      }

      try
      {
        _stoppingCts.Cancel();
      }
      finally
      {
        await Task.WhenAny(_executingTask, Task.Delay(Timeout.Infinite, cancellationToken));
      }
    }

    public virtual void Dispose()
    {
      _stoppingCts.Cancel();
    }

    /// <summary>
    /// Consume a batch of events from a kafka topic. It will consume events until either there are no more events or the max batch size, set in the configuration, has been reached.
    /// </summary>
    /// <param name="consumer">The instantiated consumer that has been connected to one or more topics</param>
    /// <returns>A readonly collection of ConsumeResults based on the type of consumer provided</returns>
    protected virtual IReadOnlyCollection<ConsumeResult<TKey, TValue>> ConsumeBatch<TKey, TValue>(IConsumer<TKey, TValue> consumer)
    {
      var message = consumer.Consume(_stoppingCts.Token);

      if (message?.Message is null)
        return Array.Empty<ConsumeResult<TKey, TValue>>();

      var messageBatch = new List<ConsumeResult<TKey, TValue>> { message };

      while (messageBatch.Count < MaxBatchSize)
      {
        message = consumer.Consume(TimeSpan.Zero);
        if (message?.Message is null)
          break;

        messageBatch.Add(message);
      }

      return messageBatch;
    }

    /// <summary>
    /// Take in a batch of events and iterate over them invoking a provided Func for each message to process
    /// </summary>
    /// <param name="batchResults">A collection of events that have been consumed.</param>
    /// <param name="handleEventFuncAsync">A func that contains the custom logic to process the events</param>
    /// <param name="cancellationToken">A cancellation token</param>
    /// <returns>A readonly collection of ConsumeResults based on the type of consumer provided</returns>
    protected virtual async Task<ConsumeResult<TKey, TValue>?> ProcessBatchAsync<TKey, TValue>(IReadOnlyCollection<ConsumeResult<TKey, TValue>> consumeResultBatch, Func<TValue, CancellationToken, Task> handleEventFuncAsync, CancellationToken cancellationToken)
    {
      ConsumeResult<TKey, TValue>? lastSuccessfullyHandledResult = null;
      Exception? exceptionThrownByHandleEvent = null;

      _logger.LogInformation("Fetched batch of {MessageCount} messages, handling batch...", consumeResultBatch.Count);
      foreach (var consumeResult in consumeResultBatch)
      {
        try
        {
          await handleEventFuncAsync(consumeResult.Message.Value, cancellationToken);
          lastSuccessfullyHandledResult = consumeResult;
        }
        catch (Exception e)
        {
          exceptionThrownByHandleEvent = e;
          break;
        }
      }

      if (lastSuccessfullyHandledResult != null)
      {
        _logger.LogInformation("Offset of last successfully handled event stored");
      }

      if (exceptionThrownByHandleEvent == null)
        _logger.LogInformation("Batch handled successfully");
      else
      {
        _logger.LogWarning("Batch handling terminated before completion due to error");
        throw exceptionThrownByHandleEvent;
      }

      return lastSuccessfullyHandledResult;
    }

    private int GetBatchSize()
    {
      var maxBatchSizeString = AppConfig[EnvironmentVariables.MaxBatchSize];
      if (string.IsNullOrWhiteSpace(maxBatchSizeString))
      {
        return DEFAULT_MAX_BATCH_SIZE;
      }

      if (int.TryParse(maxBatchSizeString, out MaxBatchSize))
      {
        return MaxBatchSize;
      }
      else
      {
        _logger.LogWarning($"The configuration value 'MaxBatchSize' could not be parsed correctly.  Setting the MaxBatchSize to the default value of {DEFAULT_MAX_BATCH_SIZE}");
        return DEFAULT_MAX_BATCH_SIZE;
      }
    }

    private void SetupKafkaTopics()
    {
      var consumerTopicsConfig = AppConfig[EnvironmentVariables.ConsumerTopics];
      if (string.IsNullOrWhiteSpace(consumerTopicsConfig))
      {
        throw new ConfigurationNullException(nameof(EnvironmentVariables.ConsumerTopics));
      }
      var consumerTopics = consumerTopicsConfig.Split(',');

      foreach (var topic in consumerTopics)
      {
        TopicsToSubscribe.Add(topic);
      }
    }
  }
}
