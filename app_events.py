import os
from dotenv import load_dotenv
from event_hub.event_hub_streaming import EventHubStreaming
import asyncio

# Load Variables
load_dotenv()
CNN_STRING = os.getenv("CNN_STRING_EVENTHUB")
NAME_EVENTHUB = os.getenv("NAME_EVENTHUB")
BATCH_QUANTITY = int(os.getenv("BATCH_QUANTITY"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))
SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS"))

async def insert_batches():
    tasks = []
    try:
        for _ in range(BATCH_QUANTITY):
            event_hub_streaming = EventHubStreaming(CNN_STRING, NAME_EVENTHUB)
            tasks.append(event_hub_streaming.insert_batch_events(BATCH_SIZE))

        await asyncio.gather(*tasks)
        print("All batches inserted successfully.")
        
    except Exception as e:
        print(f"Error during batch insertion: {e}")

asyncio.run(insert_batches())