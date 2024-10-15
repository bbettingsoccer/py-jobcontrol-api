from fastapi import FastAPI
from .routes import control_execution_router as ControlExecution
from .routes import job_functional_router as JobFunctional
from .routes import job_support_router as JobSupport

from .routes import control_schedule_functional_router as ControlSchedule

app = FastAPI()
app.include_router(ControlExecution.router, tags=["Control-Execution"], prefix="/control/execution")
app.include_router(JobFunctional.router, tags=["Job-Functional"], prefix="/job_functional/")
app.include_router(JobSupport.router, tags=["Job-Support"], prefix="/job_support/")

@app.get("/", tags=["Root"])
async def read_root():
    return {"message": "Welcome to this SheduleMatch domain !"}

