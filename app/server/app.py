from fastapi import FastAPI
from .routes import control_execution_router as ControlExecution
from .routes import job_functional_router as JobFunctional
from .routes import control_schedule_router as ControlSchedule

app = FastAPI()
app.include_router(ControlExecution.router, tags=["Control-Execution"], prefix="/control/execution")
app.include_router(JobFunctional.router, tags=["Job-Functional"], prefix="/job/functional")
app.include_router(ControlSchedule.router, tags=["Control-Schedule"], prefix="/control/schedule")

@app.get("/", tags=["Root"])
async def read_root():
    return {"message": "Welcome to this SheduleMatch domain !"}

