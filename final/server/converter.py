from asyncio import Queue
from datetime import datetime
from sys import path

try:
    path.index('/home/brunengo/Escritorio/Computación II/computacion_II/final')
except ValueError:
    path.append('/home/brunengo/Escritorio/Computación II/computacion_II/final')

from common.timeWrapper import time_excecution

class Converter():
    
    def __init__(self) -> None:
        pass
    
    @time_excecution
    async def process_requests_in_queue(self, pendingRequestsQueue: Queue, processedRequestsQueue: Queue):
        requestToProcess = await pendingRequestsQueue.get()
        await processedRequestsQueue.put({
            "id": requestToProcess["id"],
            "originDbType": requestToProcess["originDbType"],
            "convertTo": requestToProcess["convertTo"],
            "data": "mockanswer"
            })
        
        return [datetime.now(), "INFO", "Request succesfully processed", {"requestID": requestToProcess["id"]}]