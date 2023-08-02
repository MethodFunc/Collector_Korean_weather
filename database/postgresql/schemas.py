from typing import Optional
from datetime import datetime
from pydantic import BaseModel


class AwsItem(BaseModel):
    DATETIME: datetime
    RAIN15: Optional[float] = None
    RAIN60: Optional[float] = None
    RAIN3H: Optional[float] = None
    RAIN6H: Optional[float] = None
    RAIN12H: Optional[float] = None
    RAIN1D: Optional[float] = None
    TEMP: Optional[float] = None
    WD1: Optional[float] = None
    WS1: Optional[float] = None
    WD10: Optional[float] = None
    WS10: Optional[float] = None
    HUMIDITY: Optional[float] = None
    HPA: Optional[float] = None

    class Config:
        orm_mode = True
