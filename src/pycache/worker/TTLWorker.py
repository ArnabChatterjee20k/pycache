import asyncio


class TTLWorker:
    def __init__(self, callback: callable, interval: int = 15):
        self._interval = interval
        self._callback = callback
        self._running = False
        self._task: asyncio.Task = None

    async def start(self):
        await self.stop()
        self._running = True
        self._task = asyncio.create_task(self._action())

    async def _action(self):
        while self._running:
            try:
                await asyncio.to_thread(self._callback)
            except Exception as e:
                print(f"TTL Worker error: {e}")
            await asyncio.sleep(self._interval)

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
