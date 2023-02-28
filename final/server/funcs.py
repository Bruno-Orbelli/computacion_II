from celery import Celery

app = Celery("funcs", broker="redis://localhost", backend="redis://localhost:6379")

@app.task
def convertSQLtoNoSQL(tableData, originType, destinationType):
    pass

@app.task
def convertNoSQLtoSQL(data, originType, destinationType):
    pass