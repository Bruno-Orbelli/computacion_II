from multiprocessing import Queue

class Converter():
    
    def __init__(self) -> None:
        pass
    
    async def process_requests_in_queue(self, pendingRequestsQueue: Queue, processedRequestsQueue: Queue):
        requestToProcess = pendingRequestsQueue.get()
        processedRequestsQueue.put("MOCK ANSWER")
        print(processedRequestsQueue.empty())