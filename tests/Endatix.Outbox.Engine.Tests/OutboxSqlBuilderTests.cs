using Endatix.Outbox.Engine;

namespace Endatix.Outbox.Engine.Tests;

public class OutboxSqlBuilderTests
{
    [Fact]
    public void Postgres_claim_uses_skip_locked_and_quotes_with_double_quotes()
    {
        var sql = new OutboxSqlBuilder(OutboxSqlDialect.PostgreSql, "OutboxMessages").ClaimSql;

        Assert.Contains("FOR UPDATE SKIP LOCKED", sql);
        Assert.Contains("LIMIT @batchSize", sql);
        Assert.Contains("RETURNING", sql);
        Assert.Contains("\"OutboxMessages\"", sql);
        Assert.Contains("\"Payload\"::text", sql); // json read as text, provider-agnostic
        Assert.DoesNotContain("[OutboxMessages]", sql);
        Assert.DoesNotContain("READPAST", sql);
    }

    [Fact]
    public void SqlServer_claim_uses_readpast_and_quotes_with_brackets()
    {
        var sql = new OutboxSqlBuilder(OutboxSqlDialect.SqlServer, "OutboxMessages").ClaimSql;

        Assert.Contains("UPDATE TOP (@batchSize)", sql);
        Assert.Contains("OUTPUT", sql);
        Assert.DoesNotContain("FOR UPDATE SKIP LOCKED", sql);
        // The hint must be on the FROM-clause table, not on the UPDATE-clause alias.
        Assert.Contains("FROM [OutboxMessages] o WITH (READPAST, UPDLOCK, ROWLOCK)", sql);
        Assert.DoesNotContain("o WITH (READPAST, UPDLOCK, ROWLOCK) SET", sql);
        Assert.Contains("CAST(inserted.[Payload] AS nvarchar(max))", sql); // json read as text, provider-agnostic
    }

    [Theory]
    [InlineData(OutboxSqlDialect.PostgreSql)]
    [InlineData(OutboxSqlDialect.SqlServer)]
    public void Claim_filters_pending_and_respects_lease_and_next_attempt(OutboxSqlDialect dialect)
    {
        var sql = new OutboxSqlBuilder(dialect, "OutboxMessages").ClaimSql;

        Assert.Contains($"{Col(dialect, OutboxSchema.Status)} = 0", sql);      // Pending
        Assert.Contains(OutboxSchema.LockedUntil, sql);
        Assert.Contains(OutboxSchema.NextAttemptAt, sql);
        Assert.Contains("@now", sql);
        Assert.Contains("@lockedBy", sql);
        Assert.Contains("@lockedUntil", sql);
    }

    [Theory]
    [InlineData(OutboxSqlDialect.PostgreSql)]
    [InlineData(OutboxSqlDialect.SqlServer)]
    public void Claim_projects_every_IOutboxMessage_column_in_order(OutboxSqlDialect dialect)
    {
        var sql = new OutboxSqlBuilder(dialect, "OutboxMessages").ClaimSql;

        // The reader in SqlOutboxClaimStore reads by ordinal — this order must hold.
        string[] ordered =
        [
            OutboxSchema.Id, OutboxSchema.EventType, OutboxSchema.Payload, OutboxSchema.TenantId,
            OutboxSchema.OccurredAt, OutboxSchema.SchemaVersion, OutboxSchema.Attempts, OutboxSchema.TraceId,
        ];
        var lastIndex = -1;
        foreach (var col in ordered)
        {
            var idx = sql.LastIndexOf(Col(dialect, col), StringComparison.Ordinal);
            Assert.True(idx > lastIndex, $"Column {col} not found after the previous projection column.");
            lastIndex = idx;
        }
    }

    [Theory]
    [InlineData(OutboxSqlDialect.PostgreSql)]
    [InlineData(OutboxSqlDialect.SqlServer)]
    public void MarkSent_sets_sent_status_releases_lease_and_guards_pending(OutboxSqlDialect dialect)
    {
        var sql = new OutboxSqlBuilder(dialect, "OutboxMessages").MarkSentSql;

        Assert.Contains($"{Col(dialect, OutboxSchema.Status)} = 1", sql);       // Sent
        Assert.Contains($"{Col(dialect, OutboxSchema.LockedUntil)} = NULL", sql);
        Assert.Contains($"{Col(dialect, OutboxSchema.LockedBy)} = NULL", sql);
        Assert.Contains($"WHERE {Col(dialect, OutboxSchema.Id)} = @id AND {Col(dialect, OutboxSchema.Status)} = 0", sql);
        Assert.Contains($"AND {Col(dialect, OutboxSchema.LockedBy)} = @lockedBy", sql); // lease-ownership guard
    }

    [Theory]
    [InlineData(OutboxSqlDialect.PostgreSql)]
    [InlineData(OutboxSqlDialect.SqlServer)]
    public void Reschedule_increments_attempts_sets_gate_and_guards_pending(OutboxSqlDialect dialect)
    {
        var sql = new OutboxSqlBuilder(dialect, "OutboxMessages").RescheduleSql;
        var attempts = Col(dialect, OutboxSchema.Attempts);

        Assert.Contains($"{attempts} = {attempts} + 1", sql);
        Assert.Contains($"{Col(dialect, OutboxSchema.NextAttemptAt)} = @nextAttemptAt", sql);
        Assert.Contains($"{Col(dialect, OutboxSchema.Status)} = 0", sql);       // pending guard
        Assert.Contains($"AND {Col(dialect, OutboxSchema.LockedBy)} = @lockedBy", sql); // lease-ownership guard
    }

    [Theory]
    [InlineData(OutboxSqlDialect.PostgreSql)]
    [InlineData(OutboxSqlDialect.SqlServer)]
    public void MarkFailed_increments_attempts_sets_failed_and_guards_pending(OutboxSqlDialect dialect)
    {
        var sql = new OutboxSqlBuilder(dialect, "OutboxMessages").MarkFailedSql;

        Assert.Contains($"{Col(dialect, OutboxSchema.Status)} = 2", sql);       // Failed
        Assert.Contains($"WHERE {Col(dialect, OutboxSchema.Id)} = @id AND {Col(dialect, OutboxSchema.Status)} = 0", sql);
        Assert.Contains($"AND {Col(dialect, OutboxSchema.LockedBy)} = @lockedBy", sql); // lease-ownership guard
    }

    [Fact]
    public void Qualified_table_name_quotes_each_part()
    {
        var pg = new OutboxSqlBuilder(OutboxSqlDialect.PostgreSql, "app.OutboxMessages").ClaimSql;
        var ss = new OutboxSqlBuilder(OutboxSqlDialect.SqlServer, "app.OutboxMessages").ClaimSql;

        Assert.Contains("\"app\".\"OutboxMessages\"", pg);
        Assert.Contains("[app].[OutboxMessages]", ss);
    }

    private static string Col(OutboxSqlDialect dialect, string name) =>
        dialect == OutboxSqlDialect.SqlServer ? $"[{name}]" : $"\"{name}\"";
}
