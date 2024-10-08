from pydantic import BaseModel, Field


class ResultTPCDS(BaseModel):
    sql_name: str
    execution_time: float = 0.0
