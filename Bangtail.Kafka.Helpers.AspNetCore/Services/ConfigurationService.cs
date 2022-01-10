using System;
using System.IO;
using System.Linq;
using Bangtail.Kafka.Helpers.AspNetCore.Constants;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace Bangtail.Kafka.Helpers.AspNetCore.Services
{
  public class ConfigurationService : IConfigurationService
  {

    public IConfiguration GetConfiguration()
    {
      var environmentName = Environment.GetEnvironmentVariable(EnvironmentVariables.AspnetCoreEnvironment);
      return new ConfigurationBuilder()
                        .SetBasePath(Directory.GetCurrentDirectory())
                        .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                        .AddJsonFile($"appsettings.{environmentName}.json", optional: true)
                        .AddEnvironmentVariables()
                        .Build();
    }

    /// <summary>
    /// Method takes in an IConfiguration and builds a ConsumerConfig from the "Kafka" section of the configuration
    /// </summary>
    /// <param name="config">IConfiguration</param>
    /// <returns>ConsumerConfig</returns>
    public ConsumerConfig GetConsumerConfig(IConfiguration config)
    {
      var kafkaConfig = config.GetSection("Kafka").GetChildren().ToDictionary(x => x.Key, x => x.Value);
      return new ConsumerConfig(kafkaConfig);
    }
  }
}
