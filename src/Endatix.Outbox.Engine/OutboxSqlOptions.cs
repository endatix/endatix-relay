namespace Endatix.Outbox.Engine;

/// <summary>Configuration for the SQL claim store: which dialect to emit and the table to target.</summary>
public sealed class OutboxSqlOptions
{
    /// <summary>SQL dialect of the target database.</summary>
    public OutboxSqlDialect Dialect { get; set; }

    /// <summary>
    /// Table name (optionally schema/prefix-qualified, e.g. <c>"app.OutboxMessages"</c>). Defaults to
    /// <see cref="OutboxSchema.DefaultTable"/>. Both hosts (API + worker) must agree on this value.
    /// </summary>
    public string TableName { get; set; } = OutboxSchema.DefaultTable;
}
