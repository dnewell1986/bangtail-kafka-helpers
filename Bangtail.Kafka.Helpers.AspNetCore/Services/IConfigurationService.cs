using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace Bangtail.Kafka.Helpers.AspNetCore.Services
{
  public interface IConfigurationService
  {
    IConfiguration GetConfiguration();
    ConsumerConfig GetConsumerConfig(IConfiguration config);
  }
}
