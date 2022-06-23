import websockets
import json
import asyncio


class RPCResponse:
    def __init__(self, id, result=None, error=None):
        self.id = id
        self.result = result
        self.error = error

    def is_error():
        return self.error is not None


class Eventhub:
    def __init__(self, url, jwt):
        self.url = url
        self.jwt = jwt
        self._rpc_awaitables = dict()
        self._subscription_callbacks = dict()
        self._subscription_id_map = dict()
        self._rpc_id_counter = 0
        self._websocket = None

    async def __rpc_request(self, method, params):
        payload = {
            "id": self._rpc_id_counter,
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }

        await self._websocket.send(json.dumps(payload))

        self._rpc_id_counter += 1
        return payload["id"]

    async def __wait_for_response(self, rpcId):
        self._rpc_awaitables[rpcId] = asyncio.Future()

        resp = await asyncio.wait_for(self._rpc_awaitables[rpcId], 5)
        del self._rpc_awaitables[rpcId]
        return resp

    async def __rpc_request_wait_for_response(self, method, params):
        rpcId = await self.__rpc_request(method, params)
        return await self.__wait_for_response(rpcId)

    async def __read(self):
        try:
            data = await self._websocket.recv()
            # print("Received: %s" %(data))
            resp = json.loads(data)
            rpcId = resp["id"]
            if rpcId in self._rpc_awaitables:
                self._rpc_awaitables[rpcId].set_result(resp)
            elif rpcId in self._subscription_callbacks:
                asyncio.create_task(
                    self._subscription_callbacks[rpcId](
                        resp["result"]["topic"], resp["result"]["message"]
                    )
                )
        except:
            pass

    async def connect(self):
        self._websocket = await websockets.connect(self.url)
        asyncio.create_task(self.consume())
        return self.is_connected()

    async def consume(self):
        if self.is_connected():
            await asyncio.create_task(self.__read())

            if len(self._rpc_awaitables) > 0 or len(self._subscription_callbacks) > 0:
                asyncio.create_task(self.consume())
        else:
            # print("Disconnected")
            await asyncio.sleep(1)

    async def disconnect(self):
        try:
            await asyncio.wait_for(
                self.__rpc_request_wait_for_response("DISCONNECT", {}), 1
            )
            self._websocket.close()
            self.is_connected = False
            self._rpc_id_counter = 0
            self._subscription_callbacks.clear()
        except:
            pass

    def is_connected(self):
        if self._websocket is not None:
            return self._websocket.open
        return False

    async def subscribe(self, topic, callback):
        resp = await self.__rpc_request_wait_for_response("SUBSCRIBE", {"topic": topic})

        if resp["result"]["action"] == "subscribe" and resp["result"]["status"] == "ok":
            self._subscription_callbacks[resp["id"]] = callback
            self._subscription_id_map.setdefault(topic, []).append(resp["id"])

        return resp

    async def unsubscribe(self, topic):
        if topic in self._subscription_id_map:
            for id in self._subscription_id_map[topic]:
                del self._subscription_callbacks[id]

        return await self.__rpc_request_wait_for_response("UNSUBSCRIBE", [topic])

    async def unsubscribe_all(self):
        self._subscriptionCallbacks.clear()
        return await self.__rpc_request_wait_for_response("UNSUBSCRIBEALL", {})

    async def list_subscriptions(self):
        resp = await self.__rpc_request_wait_for_response("LIST", {})
        return resp["result"]

    async def publish(self, topic, message):
        return await self.__rpc_request_wait_for_response(
            "PUBLISH", {"topic": topic, "message": message}
        )

    async def set(self, key, value):
        return await self.__rpc_request_wait_for_response(
            "SET", {"key": key, "value": value}
        )

    async def get(self, key):
        res = await self.__rpc_request_wait_for_response("GET", {"key": key})

        return res["result"]["value"]

    async def delete(self, key):
        return await self.__rpc_request_wait_for_response("DEL", {"key": key})
