# Bangtail.Kafka.Helpers

This project provides a package that makes it easy to setup a Kafka Client as a background service.  It utilizes the [confluent-kafka-dotnet](https://github.com/confluentinc/confluent-kafka-dotnet) packages to help with configuration and setup.  For example usage refer to [ConsumerHostedService.cs](Examples/Bangtail.Kafka.Example.Consumer/ConsumerHostedService.cs).  This shows how to use the abstract base [ConsumerBackgroundService.cs](Bangtail.Kafka.Helpers.AspNetCore/ConsumerBackgroundService.cs) to setup the background service.

The base class with extract and setup necessary configuration from the `appsettings.json` or configured environment variables.

## Usage

There are three configuration properties that are necessary for this project to start up correctly.
1. `ConsumerTopics` - This property is where to define the topics your consumer will subscribe to.  You can provide a single topic name or a comma separated list.
2. `MaxBatchSize` - This property is where you can set the maximum number of events that the consumer will read from Kafka before starting to process them. Defaults to 50.
3. `Kafka: { group.id }` - This is needed when setting a ConsumerConfig object.
4. `Kafka: { auto.offset.reset }` - This is needed when setting a ConsumerConfig object.

## ConsumerBackgroundService

This is an abstract class that should be used to implement a custom Kafka consumer.  It is based on the IHostedService interface for hosting long running tasks. It also provides two protected methods for managing the consume and handle event processes.

### `ConsumeBatch(consumer)`

This method will consume a batch of events from a kafka topic.  It will consume events until either there are no more events or the `max batch size`, set in the configuration, has been reached.  It will return an `IReadOnlyCollection<ConsumeResult<TKey, TValue>>` which will contain the batch of events that can be iterated over and processed. Handling these batches can be done by the `ProcessBatchAsync` method outlined below.

### `ProcessBatchAsync(batchResults, handleEventFuncAsync, cancellationToken)`

This method will take in a batch of events, a Func that contains the custom logic to process the events, and a CancellationToken.  It will iterate over the batch of events and invoke the Func for each message to process.  This method will return the last event that was successfully consumed so that the offset of the event can be committed back to Kafka.

## Contributing

Please see the [Contributing](CONTRIBUTING.md) page to get information about how you can contribute.

## License

MIT License. Info can be found at [LICENSE.md](LICENSE.md)