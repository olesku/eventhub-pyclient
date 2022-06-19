import unittest
from eventhub_client import Eventhub
import asyncio

class TestEventhubClient(unittest.TestCase):
  def test_is_connected(self):
    eventhub = Eventhub("ws://127.0.0.1:8080", "myJWT")

    self.assertFalse(eventhub.is_connected())

if __name__ == '__main__':
  unittest.main()