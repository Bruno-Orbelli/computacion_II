from celery import Celery
import math

app = Celery("funciones", broker="redis://localhost", backend="redis://localhost:6379")

@app.task
def raiz(num):
   return round(math.sqrt(num), 5)

@app.task
def pot(num):
   return round(num ** 2, 5)

@app.task
def log(num):
   return round(math.log10(num), 5)

if __name__ == "__main__":
    app.start()