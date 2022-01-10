using Bangtail.Kafka.Helpers.AspNetCore.Services;
using Microsoft.Extensions.DependencyInjection;

namespace Bangtail.Kafka.Helpers.AspNetCore.Extensions
{
  public static class IServiceCollectionExtensions
  {
    public static IServiceCollection AddCommonKafkaServices(this IServiceCollection services)
    {
      services.AddTransient<IConfigurationService, ConfigurationService>();
      return services;
    }
  }
}
