import asyncio
import json
import os
import tempfile
import hashlib
import time

CACHEFILE = "llm_cache.json"
REQUEST_TIMEOUT = 80  # increase for longer LLM calls
VERBOSE = False

class JSONCacheAsync(object):
    def __init__(self, path: str = CACHEFILE):
        self.lock = asyncio.Lock()
        self.path = path
        if os.path.isfile(path):
            with open(path, "r") as fp:
                self.cache = json.load(fp)
        else:
            self.cache = {}

    async def set(self, key, value):
        async with self.lock:
            self.cache[key] = value
            self.write_cache()

    async def get(self, key):
        return self.cache.get(key)

    def write_cache(self):
        tmp_dir = "tmp"
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)
        with tempfile.NamedTemporaryFile("w", dir=tmp_dir, delete=False) as fp:
            json.dump(self.cache, fp, indent=4)
        # os.rename(fp.name, self.path)
        os.replace(fp.name, self.path)


class RateLimiter(object):
    """
    Rate limiter and timeouts for API calls
    """

    def __init__(self):
        self.timings = {
            "default": {
                "dt_min": 0.001,
                "dt_max": 0.05,
                "last_request": time.monotonic(),
            }
        }
        self.last_slowdown = time.monotonic()
        self.lock = asyncio.Lock()

    def dt_mid(self, fn):
        return (self.timings[fn]["dt_min"] + self.timings[fn]["dt_max"]) / 2

    async def call(self, f, *args, **kwargs):
        fn = f.__qualname__
        if fn not in self.timings:
            self.timings[fn] = self.timings["default"]
        while True:
            t = time.monotonic()
            if (t - self.timings[fn]["last_request"]) > self.dt_mid(fn):
                self.timings[fn]["last_request"] = t
                break
            await asyncio.sleep(self.timings[fn]["dt_min"])
        try:
            coro = f(*args, **kwargs)
            res = await asyncio.wait_for(coro, REQUEST_TIMEOUT)
            await self.speed_up(fn)
        except Exception as e:
            if VERBOSE:
                print(f"timeout exception: {fn} \n{e}")
                print(*args)
                print("------------")
            await self.slow_down(fn)
            res = await self.call(f, *args, **kwargs)
        return res

    async def slow_down(self, fn):
        async with self.lock:
            t_now = time.monotonic()
            if t_now - self.last_slowdown > 0.01:
                self.last_slowdown = t_now
                self.timings[fn]["dt_min"] = self.dt_mid(fn)
                self.timings[fn]["dt_max"] *= 1.01
                if VERBOSE:
                    print(
                        f"slow down: {fn} {self.timings[fn]['dt_min']} {self.timings[fn]['dt_max']}\n------------"
                    )

    async def speed_up(self, fn):
        async with self.lock:
            self.timings[fn]["dt_max"] = self.dt_mid(fn)
            self.timings[fn]["dt_min"] *= 0.999
            if VERBOSE:
                print(
                    f"speed up: {fn} {self.timings[fn]['dt_min']} {self.timings[fn]['dt_max']}\n------------"
                )