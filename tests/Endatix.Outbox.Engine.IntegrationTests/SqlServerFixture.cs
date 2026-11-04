using System.Runtime.InteropServices;
using Testcontainers.MsSql;

namespace Endatix.Outbox.Engine.IntegrationTests;

/// <summary>
/// Spins up a real SQL Server container once per test collection, like <see cref="PostgresFixture"/>; each
/// test creates its own uniquely-named outbox table. The image is amd64-only, so on Arm64 the container is
/// never started and the tests skip (<see cref="SqlServerFactAttribute"/>).
/// </summary>
public sealed class SqlServerFixture : IAsyncLifetime
{
    private readonly MsSqlContainer? _container = SqlServerFactAttribute.CanRun
        ? new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-CU21-ubuntu-22.04").Build()
        : null;

    public string ConnectionString => _container?.GetConnectionString()
        ?? throw new InvalidOperationException("SQL Server tests don't run on Arm64.");

    public Task InitializeAsync() => _container?.StartAsync() ?? Task.CompletedTask;

    public Task DisposeAsync() => _container?.DisposeAsync().AsTask() ?? Task.CompletedTask;
}

[CollectionDefinition(Name)]
public sealed class SqlServerCollection : ICollectionFixture<SqlServerFixture>
{
    public const string Name = "sqlserver";
}

/// <summary>A fact that skips on Arm64, where the amd64-only SQL Server image can't run.</summary>
public sealed class SqlServerFactAttribute : FactAttribute
{
    public static bool CanRun => RuntimeInformation.OSArchitecture != Architecture.Arm64;

    public SqlServerFactAttribute()
    {
        if (!CanRun)
        {
            Skip = "The SQL Server image is amd64-only; CI runs these tests on x64.";
        }
    }
}
