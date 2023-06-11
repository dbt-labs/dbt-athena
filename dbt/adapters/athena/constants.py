from dbt.events import AdapterLogger

DEFAULT_THREAD_COUNT = 4
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_POLLING_INTERVAL = 5
DEFAULT_SPARK_ENGINE_CONFIG = {"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 2, "DefaultExecutorDpuSize": 1}
DEFAULT_SPARK_SESSION_TIMEOUT = 15 * 60

LOGGER = AdapterLogger(__name__)
