namespace Endatix.Outbox.Engine;

/// <summary>
/// Builds the claim-store SQL from <see cref="OutboxSchema"/> + <see cref="OutboxStatus"/> for a given
/// <see cref="OutboxSqlDialect"/> and table name. Pure and side-effect-free so it can be unit-tested without
/// a database. The projection (claim SELECT/OUTPUT) returns exactly the <see cref="IOutboxMessage"/> columns,
/// in the order <see cref="SqlOutboxClaimStore"/> reads them.
/// </summary>
internal sealed class OutboxSqlBuilder
{
    private readonly OutboxSqlDialect _dialect;
    private readonly string _table;

    /// <summary>Ordered projection of the columns that materialize an <see cref="IOutboxMessage"/>.</summary>
    private static readonly string[] ProjectionColumns =
    [
        OutboxSchema.Id, OutboxSchema.EventType, OutboxSchema.Payload, OutboxSchema.TenantId,
        OutboxSchema.OccurredAt, OutboxSchema.SchemaVersion, OutboxSchema.Attempts, OutboxSchema.TraceId,
    ];

    public OutboxSqlBuilder(OutboxSqlDialect dialect, string tableName)
    {
        if (!Enum.IsDefined(dialect))
        {
            throw new ArgumentOutOfRangeException(nameof(dialect), dialect, "Unsupported SQL dialect.");
        }

        _dialect = dialect;
        _table = QuoteQualified(tableName);
        ClaimSql = BuildClaimSql();
        MarkSentSql = BuildMarkSentSql();
        RescheduleSql = BuildRescheduleSql();
        MarkFailedSql = BuildMarkFailedSql();
    }

    /// <summary>Claims a batch (skip-locked + lease) and returns the projected rows.</summary>
    public string ClaimSql { get; }

    /// <summary>Marks a pending row sent and releases its lease.</summary>
    public string MarkSentSql { get; }

    /// <summary>Increments attempts, sets the next-attempt gate, releases the lease (row stays pending).</summary>
    public string RescheduleSql { get; }

    /// <summary>Increments attempts, marks the row failed, releases its lease.</summary>
    public string MarkFailedSql { get; }

    private string BuildClaimSql()
    {
        var pending = (int)OutboxStatus.Pending;
        var claimable =
            $"SELECT {Q(OutboxSchema.Id)} FROM {_table} " +
            $"WHERE {Q(OutboxSchema.Status)} = {pending} " +
            $"AND ({Q(OutboxSchema.LockedUntil)} IS NULL OR {Q(OutboxSchema.LockedUntil)} <= @now) " +
            $"AND ({Q(OutboxSchema.NextAttemptAt)} IS NULL OR {Q(OutboxSchema.NextAttemptAt)} <= @now)";

        if (_dialect == OutboxSqlDialect.PostgreSql)
        {
            return
                $"WITH claimable AS ({claimable} ORDER BY {Q(OutboxSchema.Id)} LIMIT @batchSize FOR UPDATE SKIP LOCKED) " +
                $"UPDATE {_table} o SET {Q(OutboxSchema.LockedBy)} = @lockedBy, {Q(OutboxSchema.LockedUntil)} = @lockedUntil " +
                $"FROM claimable c WHERE o.{Q(OutboxSchema.Id)} = c.{Q(OutboxSchema.Id)} " +
                $"RETURNING {Projection("o")};";
        }

        // SQL Server: READPAST is the SKIP LOCKED equivalent; OUTPUT returns the claimed rows.
        // The table hint MUST sit on the table source in the FROM clause — not on the UPDATE-clause
        // alias (which the FROM defines), or it is rejected/ignored and skip-locked is lost.
        return
            $"UPDATE TOP (@batchSize) o " +
            $"SET {Q(OutboxSchema.LockedBy)} = @lockedBy, {Q(OutboxSchema.LockedUntil)} = @lockedUntil " +
            $"OUTPUT {Projection("inserted")} FROM {_table} o WITH (READPAST, UPDLOCK, ROWLOCK) " +
            $"WHERE o.{Q(OutboxSchema.Status)} = {pending} " +
            $"AND (o.{Q(OutboxSchema.LockedUntil)} IS NULL OR o.{Q(OutboxSchema.LockedUntil)} <= @now) " +
            $"AND (o.{Q(OutboxSchema.NextAttemptAt)} IS NULL OR o.{Q(OutboxSchema.NextAttemptAt)} <= @now);";
    }

    // The claim projection, qualified by the row source ("o" for RETURNING, "inserted" for OUTPUT), in the
    // exact order SqlOutboxClaimStore reads by ordinal. The JSON Payload column is projected AS TEXT so the
    // reader can GetString it regardless of how the provider surfaces json/jsonb (SQL Server's native `json`
    // type, or a host that enabled Npgsql dynamic-JSON mapping).
    private string Projection(string source) =>
        string.Join(", ", ProjectionColumns.Select(col => ProjectColumn(source, col)));

    private string ProjectColumn(string source, string column)
    {
        var qualified = $"{source}.{Q(column)}";
        if (column != OutboxSchema.Payload)
        {
            return qualified;
        }

        return _dialect == OutboxSqlDialect.SqlServer
            ? $"CAST({qualified} AS nvarchar(max))"
            : $"{qualified}::text";
    }

    private string BuildMarkSentSql() =>
        $"UPDATE {_table} SET {Q(OutboxSchema.Status)} = {(int)OutboxStatus.Sent}, " +
        $"{Q(OutboxSchema.ProcessedAt)} = @now, {ReleaseLease()} {PendingGuard()}";

    private string BuildRescheduleSql() =>
        $"UPDATE {_table} SET {Q(OutboxSchema.Attempts)} = {Q(OutboxSchema.Attempts)} + 1, " +
        $"{Q(OutboxSchema.NextAttemptAt)} = @nextAttemptAt, {ReleaseLease()} {PendingGuard()}";

    private string BuildMarkFailedSql() =>
        $"UPDATE {_table} SET {Q(OutboxSchema.Attempts)} = {Q(OutboxSchema.Attempts)} + 1, " +
        $"{Q(OutboxSchema.Status)} = {(int)OutboxStatus.Failed}, {Q(OutboxSchema.ProcessedAt)} = @now, " +
        $"{ReleaseLease()} {PendingGuard()}";

    // Only the current lease owner may mutate a pending row — replaces the entity-level EnsurePending guard
    // with a DB-enforced one that also checks lease ownership, so an expired-lease instance can't clobber a
    // row another instance has re-claimed.
    private string PendingGuard() =>
        $"WHERE {Q(OutboxSchema.Id)} = @id AND {Q(OutboxSchema.Status)} = {(int)OutboxStatus.Pending} " +
        $"AND {Q(OutboxSchema.LockedBy)} = @lockedBy;";

    private string ReleaseLease() =>
        $"{Q(OutboxSchema.LockedUntil)} = NULL, {Q(OutboxSchema.LockedBy)} = NULL";

    private string Q(string identifier) => _dialect switch
    {
        OutboxSqlDialect.SqlServer => $"[{identifier}]",
        _ => $"\"{identifier}\"",
    };

    private string QuoteQualified(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Table name must be non-empty.", nameof(name));
        }

        var segments = name.Split('.');
        if (segments.Any(string.IsNullOrWhiteSpace))
        {
            throw new ArgumentException($"Table name '{name}' has an empty segment.", nameof(name));
        }

        return string.Join(".", segments.Select(Q));
    }
}
