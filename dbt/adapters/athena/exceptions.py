from dbt.exceptions import CompilationError
from dbt.exceptions import DbtRuntimeError


class SnapshotMigrationRequired(CompilationError):
    """Hive snapshot requires a manual operation due to backward incompatible changes."""


class S3LocationException(DbtRuntimeError):
    pass
