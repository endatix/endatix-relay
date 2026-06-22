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
        var projection = string.Join(", ", ProjectionColumns.Select(Q));
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
                $"RETURNING {string.Join(", ", ProjectionColumns.Select(col => "o." + Q(col)))};";
        }

        // SQL Server: READPAST is the SKIP LOCKED equivalent; OUTPUT returns the claimed rows.
        var output = string.Join(", ", ProjectionColumns.Select(col => "inserted." + Q(col)));
        return
            $"UPDATE TOP (@batchSize) o WITH (READPAST, UPDLOCK, ROWLOCK) " +
            $"SET {Q(OutboxSchema.LockedBy)} = @lockedBy, {Q(OutboxSchema.LockedUntil)} = @lockedUntil " +
            $"OUTPUT {output} FROM {_table} o " +
            $"WHERE o.{Q(OutboxSchema.Status)} = {pending} " +
            $"AND (o.{Q(OutboxSchema.LockedUntil)} IS NULL OR o.{Q(OutboxSchema.LockedUntil)} <= @now) " +
            $"AND (o.{Q(OutboxSchema.NextAttemptAt)} IS NULL OR o.{Q(OutboxSchema.NextAttemptAt)} <= @now);";
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

    // Only pending rows are mutable — replaces the entity-level EnsurePending guard with a DB-enforced one.
    private string PendingGuard() =>
        $"WHERE {Q(OutboxSchema.Id)} = @id AND {Q(OutboxSchema.Status)} = {(int)OutboxStatus.Pending};";

    private string ReleaseLease() =>
        $"{Q(OutboxSchema.LockedUntil)} = NULL, {Q(OutboxSchema.LockedBy)} = NULL";

    private string Q(string identifier) => _dialect switch
    {
        OutboxSqlDialect.SqlServer => $"[{identifier}]",
        _ => $"\"{identifier}\"",
    };

    private string QuoteQualified(string name) => string.Join(".", name.Split('.').Select(Q));
}
