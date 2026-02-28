# 環境変数の読み込みと管理
from typing import Optional

try:
    # Pydantic v2 系
    from pydantic_settings import BaseSettings
    from pydantic import Field
except ImportError:
    # 互換: v1 をお使いの場合（必要なら pip install "pydantic<2"）
    from pydantic import BaseSettings, Field  # type: ignore[assignment]

class Settings(BaseSettings):
    # Gigya
    GIGYA_BASE: str
    GIGYA_API_KEY: str

    # Xarvio
    XARVIO_TOKEN_API_URL: str = Field(..., alias="XARVIO_TOKEN_API_URL")

    # GraphQL（キー名の揺れを吸収）
    XARVIO_GRAPHQL_ENDPOINT: Optional[str] = None
    GRAPHQL_END_POINT: Optional[str] = None

    # Snapshot job / storage
    HFR_SNAPSHOT_DATABASE_URL: Optional[str] = None
    DATABASE_URL: Optional[str] = None
    SNAPSHOT_USER_EMAIL: Optional[str] = None
    SNAPSHOT_USER_PASSWORD: Optional[str] = None
    SNAPSHOT_JOB_SECRET: Optional[str] = None

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"

    @property
    def GRAPHQL_ENDPOINT(self) -> str:
        return (
            self.XARVIO_GRAPHQL_ENDPOINT
            or self.GRAPHQL_END_POINT
            or "https://fm-api.xarvio.com/api/graphql/data"
        )

settings = Settings()
