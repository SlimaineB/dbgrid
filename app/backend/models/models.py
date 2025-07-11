from pydantic import BaseModel

class SQLRequest(BaseModel):
    query: str
    profiling: bool = False
    max_rows: int = 50
    num_threads: int = -1

class S3PathRequest(BaseModel):
    s3_path: str

class SuggestPartitionRequest(BaseModel):
    s3_path: str
    threshold: int = 10

class PartitionValueCountRequest(BaseModel):
    s3_path: str
    column: str


class SQLAnalyzerRequest(BaseModel):
    sql: str
