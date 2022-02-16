import unittest
from eventhub_client.client import EventHub
import asyncio

class TestEventhubClient(unittest.TestCase):
  def test_is_connected(self):
    eventhub = EventHub("ws://127.0.0.1:8080", "myJWT")

    self.assertFalse(eventhub.is_connected())
    loop = asyncio.new_event_loop()
    loop.run_until_complete(eventhub.run())

if __name__ == '__main__':
  unittest.main()