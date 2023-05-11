from ast import literal_eval
from asyncio import Queue, QueueEmpty
from datetime import datetime
from celery import group

from funcs import convert_table_create_statement_to_sqlite3, create_table_insert_statement_for_sqlite3

class Converter():
    
    def __init__(self) -> None:
        self.pendingResults = Queue()
    
    def build_task_group(self, requestsToProcess: 'list[dict]'):
        tasks, endOfTransmission = [], False
        
        for request in requestsToProcess:
            if request["body"] == "EOT":
                endOfTransmission = True
            
            elif request["objectType"] == "table":
                tableName = list(request["body"].keys())[0]
                
                createTaskArgs = [literal_eval(request["body"][tableName][1][0])[1], request["originDbType"], tableName]
                insertTaskArgs = [request["body"][tableName][0], request["originDbType"], tableName]

                request.pop("body")
                request.pop("originDbType")

                createTaskArgs.append(request)
                insertTaskArgs.append(request)
                
                tasks.extend([convert_table_create_statement_to_sqlite3.s(*createTaskArgs), create_table_insert_statement_for_sqlite3.s(*insertTaskArgs)])
        
        return (tasks, endOfTransmission)
    
    async def process_requests_in_queue(self, pendingRequestsQueue: Queue, processedRequestsQueue: Queue):
        requestsToProcess = []
        
        try:
            while True:
                requestsToProcess.append(pendingRequestsQueue.get_nowait())
        except QueueEmpty:
            pass
        
        tasks, endOfTransmission = self.build_task_group(requestsToProcess)
        
        if tasks:
            taskGroup = group(tasks)
            result = taskGroup.apply_async()
            await self.pendingResults.put(result)

        processedData = await self.check_for_results()
        processingEvents = await self.add_processed_requests(processedData, processedRequestsQueue, endOfTransmission)

        return processingEvents
    
    async def check_for_results(self):
        qSize, resultsToAdd = self.pendingResults.qsize(), []

        for _ in range(qSize):
            result = self.pendingResults.get_nowait()
            
            if result.ready():
                resultsToAdd.extend(result.get())
            else:
                await self.pendingResults.put(result)
        
        return resultsToAdd
    
    async def add_processed_requests(self, processedData: list, processedRequestsQueue: Queue, endOfTransmission: bool):
        processingEvents = []
        
        originalLen = len(processedData)
        for i in range(originalLen - 1):
            try:
                if processedData[i][0][1]["id"] == processedData[i + 1][0][1]["id"]:
                    processedData[i][0][0] = (processedData[i][0][0], processedData[i + 1][0][0])
                    processedData[i][1] += processedData[i + 1][1]
                    processedData.pop(i + 1)
            
            except IndexError:
                continue
        
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