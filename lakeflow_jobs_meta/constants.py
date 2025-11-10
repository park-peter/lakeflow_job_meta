"""Constants for the Lakeflow Jobs Meta framework"""

# Timeouts
TASK_TIMEOUT_SECONDS = 3600
JOB_TIMEOUT_SECONDS = 7200
MAX_CONCURRENT_RUNS = 1

# Task types
TASK_TYPE_NOTEBOOK = "notebook"
TASK_TYPE_SQL_QUERY = "sql_query"
TASK_TYPE_SQL_FILE = "sql_file"
TASK_TYPE_PYTHON_WHEEL = "python_wheel"
TASK_TYPE_SPARK_JAR = "spark_jar"
TASK_TYPE_PIPELINE = "pipeline"
TASK_TYPE_DBT = "dbt"

# Supported task types
SUPPORTED_TASK_TYPES = [
    TASK_TYPE_NOTEBOOK,
    TASK_TYPE_SQL_QUERY,
    TASK_TYPE_SQL_FILE,
    TASK_TYPE_PYTHON_WHEEL,
    TASK_TYPE_SPARK_JAR,
    TASK_TYPE_PIPELINE,
    TASK_TYPE_DBT,
]

# Framework paths
FRAMEWORK_PATH_PREFIX = "/frameworks/"
