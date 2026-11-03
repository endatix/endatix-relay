namespace Endatix.Outbox.Engine;

/// <summary>
/// Selects the SQL dialect for the claim store. The only real differences are the skip-locked clause
/// (<c>FOR UPDATE SKIP LOCKED</c> vs <c>READPAST</c>), identifier quoting (<c>"x"</c> vs <c>[x]</c>), and
/// the timestamp parameter type (provider inference vs <c>datetime2</c>).
/// </summary>
public enum OutboxSqlDialect
{
    /// <summary>PostgreSQL — <c>FOR UPDATE SKIP LOCKED</c>, double-quoted identifiers.</summary>
    PostgreSql,

    /// <summary>SQL Server — <c>WITH (READPAST, UPDLOCK, ROWLOCK)</c>, bracket-quoted identifiers.</summary>
    SqlServer,
}
