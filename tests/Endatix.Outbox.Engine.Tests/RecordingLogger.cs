using Microsoft.Extensions.Logging;

namespace Endatix.Outbox.Engine.Tests;

/// <summary>An always-enabled logger that keeps each formatted message, so tests can assert on log output.</summary>
internal sealed class RecordingLogger<T> : ILogger<T>
{
    private readonly List<string> _messages = [];

    public IReadOnlyList<string> Messages => _messages;

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
        _messages.Add(formatter(state, exception));
}
