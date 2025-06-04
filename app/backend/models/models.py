from pydantic import BaseModel

class SQLRequest(BaseModel):
    query: str
    profiling: bool = False
    distribued: bool = False
    max_rows: int = 50
    num_threads: int = -1 # Note: num_threads = -1 means "default" or "auto" mode, where the backend decides the number of threads
    lb_url: str = None  # Load Balancer URL for distributed queries
    force_refresh_cache: bool = False  # Whether to use cache for the query results
    

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
