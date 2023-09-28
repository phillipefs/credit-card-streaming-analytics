import os
from dotenv import load_dotenv
from event_hub.event_hub_streaming import EventHubStreaming
from time import sleep

# Load Variables
load_dotenv()
CNN_STRING      = os.getenv("CNN_STRING_EVENTHUB")
NAME_EVENTHUB   = os.getenv("NAME_EVENTHUB")
BATCH_QUANTITY  = int(os.getenv("BATCH_QUANTITY"))
BATCH_SIZE      = int(os.getenv("BATCH_SIZE"))
SLEEP_SECONDS   = int(os.getenv("SLEEP_SECONDS"))

for _ in range(BATCH_QUANTITY):
    event_hub_streaming = EventHubStreaming(CNN_STRING, NAME_EVENTHUB)
    event_hub_streaming.insert_batch_events(BATCH_SIZE)
    sleep(SLEEP_SECONDS)