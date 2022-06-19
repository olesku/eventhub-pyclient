#!/usr/bin/python
import asyncio
import json
from eventhub_client import Eventhub

client = Eventhub("ws://localhost:8080", "")

bar_count = 0

async def foo():
  res = await client.publish("topic1", "Test message")
  print("Result: %s" % (res["result"]["status"]))
  await asyncio.sleep(5)

def sub_callback1(topic, message):
  print("Callback 1 %s: %s" % (topic,message))

async def sub_callback2(topic, message):
  global bar_count
  bar_count += 1
  print("Callback 2 %s: %s" % (topic,message))
  #p1 = await client.publish("topic2", str(bar_count))
  #p2 = await client.publish("topic1", str(bar_count))

  #print("P1: " + json.dumps(p1))
  #print("P2: " + json.dumps(p2))

async def main():
  await client.connect()
  sub1 = await client.subscribe("topic1", sub_callback1)
  sub2 = await client.subscribe("topic2", sub_callback2)
  foo1 = await client.publish("topic1", "FOO")
  foo2 = await client.publish("topic2", "BAR")
  print(json.dumps(foo1))
  print(json.dumps(foo2))

  while True:
    await asyncio.sleep(1)

asyncio.run(main())