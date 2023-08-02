from datetime import datetime

from sqlalchemy import Integer, VARCHAR, Float
from sqlalchemy.orm import mapped_column, Mapped
from database.postgresql.connection import Base

from typing import Optional


def create_model(table_name):
    class AWS_DATA(Base):
        __tablename__ = table_name
        __table_args__ = {"extend_existing": True}

        id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
        DATETIME: Mapped[datetime] = mapped_column(nullable=False, unique=True)
        RAIN15: Mapped[float] = mapped_column(Float(precision=1), nullable=True)
        RAIN60: Mapped[float] = mapped_column(Float(precision=1), nullable=True)
        RAIN3H: Mapped[float] = mapped_column(Float(precision=1), nullable=True)
        RAIN6H: Mapped[float] = mapped_column(Float(precision=1), nullable=True)
        RAIN12H: Mapped[float] = mapped_column(Float(precision=1), nullable=True)
        RAIN1D: Mapped[float] = mapped_column(Float(precision=1), nullable=True)
        TEMP: Mapped[float] = mapped_column(Float(precision=1), nullable=True)
        WD1: Mapped[float] = mapped_column(Float(precision=1), nullable=True)
        WS1: Mapped[float] = mapped_column(Float(precision=1), nullable=True)
        WD10: Mapped[float] = mapped_column(Float(precision=1), nullable=True)
        WS10: Mapped[float] = mapped_column(Float(precision=1), nullable=True)
        HUMIDITY: Mapped[float] = mapped_column(Float(precision=0), nullable=True)
        HPA:  Mapped[float] = mapped_column(Float(precision=1), nullable=True)

        def __init__(self, DATETIME, RAIN15, RAIN60, RAIN3H, RAIN6H, RAIN12H, RAIN1D, TEMP,
                     WD1, WS1, WD10, WS10, HUMIDITY, HPA, *args, **kwargs):
            self.HPA = HPA
            self.HUMIDITY = HUMIDITY
            self.WS10 = WS10
            self.WD10 = WD10
            self.WS1 = WS1
            self.WD1 = WD1
            self.TEMP = TEMP
            self.RAIN1D = RAIN1D
            self.RAIN12H = RAIN12H
            self.RAIN6H = RAIN6H
            self.RAIN3H = RAIN3H
            self.RAIN60 = RAIN60
            self.RAIN15 = RAIN15
            self.DATETIME = DATETIME
            
            

    return AWS_DATA
