# eventhub-pyclient

Python client for Eventhub.

## How to install
```bash
pip install git+https://github.com/olesku/eventhub-pyclient
```

## Example

```python
import asyncio
from eventhub_client import Eventhub
import sys

client = Eventhub("ws://localhost:8080", "")

async def topic1_callback(topic, message):
  print("Message received on %s: %s" % (topic,message))
  await client.disconnect()

async def main():
  await client.connect()
  await client.subscribe("topic1", topic1_callback)
  await client.publish("topic1", "FOO")

  while client.is_connected():
    await asyncio.sleep(1)

asyncio.run(main())
```

