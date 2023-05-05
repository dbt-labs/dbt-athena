from dbt.exceptions import CompilationError


class SnapshotMigrationRequired(CompilationError):
    """Hive snapshot requires a manual operation due to backward incompatible changes."""
