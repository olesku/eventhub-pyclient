import websockets
import json
import asyncio
import urllib.parse

class InvalidResponseError(Exception):
    """Invalid response"""
    pass

class RPCResponse:
    def __init__(self, data):
        self.__parse__(data)

    def __parse__(self, data):
        resp = json.loads(data)
        self.id = resp.get("id", None)
        self.result = resp.get("result", None)
        self.error = resp.get("error", None)

        if self.result is None and self.error is None or self.result is not None and self.id is None:
            raise InvalidResponseError

    def is_error():
        return self.error is not None

class Eventhub:
    def __init__(self, url, jwt=""):
        self.url = url
        self._rpc_awaitables = dict()
        self._subscription_callbacks = dict()
        self._subscription_id_map = dict()
        self._rpc_id_counter = 0
        self._websocket = None

        if jwt:
            urlParts = urllib.parse.urlparse(url)
            qs = dict(urllib.parse.parse_qsl(urlParts.query))
            qs.update({'auth': jwt})
            self.url = urlParts._replace(query=urllib.parse.urlencode(qs)).geturl()

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
            resp = RPCResponse(data)
            if resp.id in self._rpc_awaitables:
                self._rpc_awaitables[resp.id].set_result(resp)
            elif resp.id in self._subscription_callbacks:
                asyncio.create_task(
                    self._subscription_callbacks[resp.id](
                        resp.result["topic"], resp.result["message"]
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

    async def subscribe(self, topic, callback, opts={}):
        opts["topic"] = topic
        resp = await self.__rpc_request_wait_for_response("SUBSCRIBE", opts)

        if resp.result["action"] == "subscribe" and resp.result["status"] == "ok":
            self._subscription_callbacks[resp.id] = callback
            self._subscription_id_map.setdefault(topic, []).append(resp.id)

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
        return resp.result

    async def publish(self, topic, message, opts={}):
        opts["topic"] = topic
        opts["message"] = message
        return await self.__rpc_request_wait_for_response(
            "PUBLISH", opts
        )

    async def set(self, key, value):
        return await self.__rpc_request_wait_for_response(
            "SET", {"key": key, "value": value}
        )

    async def ping(self):
     resp = await self.__rpc_request_wait_for_response("PING", {})
     return resp.result["pong"]

    async def get(self, key):
        res = await self.__rpc_request_wait_for_response("GET", {"key": key})

        return res.result["value"]

    async def delete(self, key):
        return await self.__rpc_request_wait_for_response("DEL", {"key": key})

    async def getEventlog(self, topic, opts={}):
        opts["topic"] = topic
        res = await self.__rpc_request_wait_for_response("EVENTLOG", opts)
        return res.result["items"]
