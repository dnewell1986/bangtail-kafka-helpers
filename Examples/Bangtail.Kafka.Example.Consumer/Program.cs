// See https://aka.ms/new-console-template for more information
using Bangtail.Kafka.Example.Consumer;
using Bangtail.Kafka.Helpers.AspNetCore.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        services.AddCommonKafkaServices();
        services.AddHostedService<ConsumerService>();
    })
    .Build()
    .Run();
