#!/usr/bin/python
import asyncio
import json
from eventhub_client.client import EventHub

client = EventHub("ws://localhost:8080", "")

async def foo():
  res = await client.publish("topic1", "Test message")
  print("Result: %s" % (res["result"]["status"]))
  await asyncio.sleep(5)
  asyncio.create_task(foo())

def sub_callback1(topic, message):
  print("Callback 1 %s: %s" % (topic,message))

def sub_callback2(topic, message):
  print("Callback 2 %s: %s" % (topic,message))

async def main():
  await client.connect()
  asyncio.create_task(client.consume())

  print("Subscribing to topic1")
  r = await client.subscribe("topic1", sub_callback1)
  print("SUBSCRIBE: Action: %s Status: %s" % (r["result"]["action"], r["result"]["status"]))

  print("Subscribing to topic2")
  r = await client.subscribe("topic2", sub_callback2)
  print("SUBSCRIBE: Action: %s Status: %s" % (r["result"]["action"], r["result"]["status"]))

  while True:
    await client.publish("topic1", "FOO")
    await client.publish("topic2", "BAR")
    await asyncio.sleep(1)

asyncio.run(main())
