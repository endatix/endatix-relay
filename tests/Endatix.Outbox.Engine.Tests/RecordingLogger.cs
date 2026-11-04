using Microsoft.Extensions.Logging;

namespace Endatix.Outbox.Engine.Tests;

/// <summary>An always-enabled logger that keeps each formatted message, so tests can assert on log output.</summary>
internal sealed class RecordingLogger<T> : ILogger<T>
{
    private readonly List<(LogLevel Level, string Message)> _entries = [];
    private readonly Lock _lock = new();

    public IReadOnlyList<string> Messages
    {
        get
        {
            lock (_lock)
            {
                return _entries.Select(e => e.Message).ToList();
            }
        }
    }

    public IReadOnlyList<(LogLevel Level, string Message)> Entries
    {
        get
        {
            lock (_lock)
            {
                return [.. _entries];
            }
        }
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        lock (_lock)
        {
            _entries.Add((logLevel, formatter(state, exception)));
        }
    }
}
