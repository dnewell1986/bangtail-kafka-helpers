using System;

namespace Bangtail.Kafka.Helpers.AspNetCore.Exceptions
{
  public class ConfigurationNullException : Exception
  {
    private const string exceptionMessageBase = "cannot be null.  It must be set in appsettings.json or as an Environment variable.";
    public ConfigurationNullException()
    {

    }

    public ConfigurationNullException(string variableName)
        : base($"{variableName} {exceptionMessageBase}")
    {

    }

    public ConfigurationNullException(string variableName, Exception inner)
        : base($"{variableName} {exceptionMessageBase}", inner)
    {

    }
  }
}
