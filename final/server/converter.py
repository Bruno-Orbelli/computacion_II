from asyncio import Queue

class Converter():
    
    def __init__(self) -> None:
        pass
    
    async def process_requests_in_queue(self, pendingRequestsQueue: Queue, processedRequestsQueue: Queue):
        requestToProcess = await pendingRequestsQueue.get()
        await processedRequestsQueue.put({
            "id": requestToProcess["id"],
            "originDbType": requestToProcess["originDbType"],
            "convertTo": requestToProcess["convertTo"],
            "data": "mockanswer"
            })