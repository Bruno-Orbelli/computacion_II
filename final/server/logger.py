from asyncio import Queue

class ServerLogger():
    
    def __init__(self, logPath: str = "./log.txt", errLogPath: str = "./err.txt") -> None:
        self.logPath = logPath
        self.errLogPath = errLogPath
        
        self.pendingEvents = Queue()
    
    async def add_event_to_queue(self, eventTuple: tuple) -> None:
        await self.pendingEvents.put(eventTuple)

    async def log_events(self) -> None:  # agregar excepcion cuando el archivo está protegido
        while not self.pendingEvents.empty():
            eventToLog = await self.pendingEvents.get()

            if eventToLog[1] in ("ERR", "FATAL"):
                with open(self.errLogPath, "a+") as errLog:
                    errLog.write(f"[{eventToLog[0]}] {eventToLog[1]}: {eventToLog[2]} {eventToLog[3]}.\n")
            
            else:
                with open(self.logPath, "a+") as log:
                    log.write(f"[{eventToLog[0]}] {eventToLog[1]}: {eventToLog[2]} {eventToLog[3]}.\n")
