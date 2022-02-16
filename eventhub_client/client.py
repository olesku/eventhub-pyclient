#!/usr/bin/python

import websockets
import json
import asyncio

class EventHub:
  def __init__(self, url, jwt):
      self.url = url
      self.jwt = jwt
      self._is_connected = False
      self._rpcAwaitables = dict()
      self._subscriptionCallbacks = dict()
      self._rpcIdCounter = 0

  async def __rpc_request(self, method, params):
    payload = {
      'id': self._rpcIdCounter,
      'jsonrpc': '2.0',
      'method': method,
      'params': params
    }

    await self._websocket.send(json.dumps(payload))
    self._rpcIdCounter += 1

    return payload['id']

  async def __read(self):
    data = await self._websocket.recv()
    #print("Received: %s" %(data))

    try:
      resp = json.loads(data)
      rpcId = resp['id']
      if rpcId in self._rpcAwaitables:
        self._rpcAwaitables[rpcId].set_result(resp)
      elif rpcId in self._subscriptionCallbacks:
        self._subscriptionCallbacks[rpcId](resp['result']['topic'], resp['result']['message'])
    except:
      pass


  async def __wait_for_response(self, rpcId):
    self._rpcAwaitables[rpcId] = asyncio.Future()

    # FIXME: Handle timeout.
    resp = await asyncio.wait_for(self._rpcAwaitables[rpcId], 5)
    del self._rpcAwaitables[rpcId]
    return resp

  async def connect(self):
      self._websocket = await websockets.connect(self.url)
      self._is_connected = True

      print("Connected")
      return True

  async def consume(self):
    if self.is_connected():
      await self.__read()
    else:
      await asyncio.sleep(1)

    await asyncio.create_task(self.consume())

  def disconnect(self):
    self.__rpc_request("DISCONNECT", {})
    self._websocket.close()
    self.is_connected = False
    self._rpcIdCounter = 0
    self._subscriptionCallbacks.clear()
    return

  def is_connected(self):
    return self._is_connected

  async def subscribe(self, topic, callback):
    rpcId = await self.__rpc_request("SUBSCRIBE", {
      "topic": topic
    })

    resp = await self.__wait_for_response(rpcId)

    if (resp['result']['action'] == 'subscribe'
        and resp['result']['status'] == 'ok'):
      self._subscriptionCallbacks[rpcId] = callback

    return resp

  async def unsubscribe(self, topic):
    rpcId = await self.__rpc_request("UNSUBSCRIBE", [topic])

    # FIXME: Remove topic from self._subscriptionCallbacks

    return await self.__wait_for_response(rpcId)

  async def unsubscribe_all(self):
    #self._subscriptionCallbacks.clear()
    rpcId = await self.__rpc_request("UNSUBSCRIBEALL", {})
    return await self.__wait_for_response(rpcId)

  async def list_subscriptions(self):
    rpcId = await self.__rpc_request("LIST", {})
    resp = await self.__wait_for_response(rpcId)
    return resp["result"]

  async def publish(self, topic, message):
    rpcId = await self.__rpc_request("PUBLISH", {
      "topic": topic,
      "message": message
    })

    return await self.__wait_for_response(rpcId)

  def set(self, key, value):
    return

  def get(self, key):
    return

  def delete(self, key):
    return