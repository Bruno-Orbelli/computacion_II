from asyncio import Queue
from datetime import datetime
from celery import group

from funcs import convert_table_create_statement_to_sqlite3, create_table_insert_statement_for_sqlite3

class Converter():
    
    def __init__(self) -> None:
        pass
    
    def build_task_group(self, requestsToProcess: 'list[dict]'):
        tasks, endOfTransmission = [], False
        
        for request in requestsToProcess:
            if request["body"] == "EOT":
                endOfTransmission = True
            
            elif request["objectType"] == "table":
                tableName = list(request["body"].keys())[0]
                
                createTaskArgs = [request["body"][tableName][0], request["convertTo"], tableName]
                insertTaskArgs = [request["body"][tableName][1][1], request["convertTo"], tableName]

                request.pop("body")
                request.pop("convertTo")

                createTaskArgs.append(request)
                insertTaskArgs.append(request)
                
                tasks.append(convert_table_create_statement_to_sqlite3.s(*createTaskArgs), create_table_insert_statement_for_sqlite3(*insertTaskArgs))
        
        return (tasks, endOfTransmission)
    
    async def process_requests_in_queue(self, pendingRequestsQueue: Queue, processedRequestsQueue: Queue):
        requestsToProcess = []
        
        while not pendingRequestsQueue.empty():
            requestsToProcess.append(await pendingRequestsQueue.get())
        
        tasks, endOfTransmission = self.build_task_group(requestsToProcess)
        
        if tasks:
            taskGroup = group(*tasks)
            taskGroup.apply_async()     
        
        processedData = taskGroup.get()
        processingEvents = await self.add_processed_requests(processedData, processedRequestsQueue, endOfTransmission)

        return processingEvents
    
    async def add_processed_requests(self, processedData: list, processedRequestsQueue: Queue, endOfTransmission: bool):
        processingEvents = []
        
        for result in processedData:      
            await processedRequestsQueue.put({
                "id": result[0][1]["id"],
                "originDbType": result[0][1]["originDbType"],
                "convertTo": result[0][1]["convertTo"],
                "objectType": result[0][1]["objectType"],
                "body": result[0][0]
                })
        
            processingEvents.append([datetime.now(), "INFO", "Request succesfully processed", {"requestID": result[0][1]["id"], "timeTaken": f"{result[1] * 10**3:.03f}ms"}])
        
        if endOfTransmission:
            await processedRequestsQueue.put({
                "id": None,
                "originDbType": None,
                "convertTo": None,
                "objectType": None,
                "body": "EOT"
            })
        
        return processingEvents