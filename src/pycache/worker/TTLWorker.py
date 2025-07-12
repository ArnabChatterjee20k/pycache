import asyncio


class TTLWorker:
    def __init__(self, callback: callable, interval: int = 15):
        self._interval = interval
        self._callback = callback
        self._running = False
        self._task: asyncio.Task = None

    def start(self):
        self.stop()
        self._running = True
        self._task = asyncio.create_task(self._action())

    async def _action(self):
        while self._running:
            self._callback()
            asyncio.sleep(self._interval)

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
