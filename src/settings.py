from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    consumer_name: str = 'selector0'
    max_workers: int = 16
    crawler_uri: str = 'http://localhost:8000'
    redis_uri: str = 'redis://localhost:6379'
    log_level: str = 'INFO'
