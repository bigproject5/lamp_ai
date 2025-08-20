import os

# Corrected getenv function to match its usage pattern (key, default_value)
def getenv(key, default=None):
    """
    Gets an environment variable, returning a default value if not found.
    """
    return os.getenv(key, default)

KAFKA_BOOTSTRAP_SERVERS = getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# For this line, we want to check KAFKA_SOURCE_TOPIC first, then default to "test-started"
KAFKA_SOURCE_TOPIC = getenv("KAFKA_SOURCE_TOPIC", "test-started")

# For this line, the logic is: check KAFKA_TOPIC, then KAFKA_RESULT_TOPIC, then default.
# We can achieve this with a nested call.
KAFKA_RESULT_TOPIC = getenv("KAFKA_TOPIC", getenv("KAFKA_RESULT_TOPIC", "ai-diagnosis-completed"))

KAFKA_GROUP_ID = getenv("KAFKA_GROUP_ID", "lamp-ai-develop")
SERVICE_NAME = getenv("SERVICE_NAME", "lamp_ai")

# The float() call will now work because getenv will return "0.5" if the env var is not set.
PREDICT_THRESHOLD = float(getenv("PREDICT_THRESHOLD", "0.5"))
