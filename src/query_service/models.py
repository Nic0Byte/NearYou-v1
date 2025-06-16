"""Modelli per Query Service."""
from typing import List, Dict, Any, Optional
from datetime import datetime, date
from pydantic import BaseModel, Field

# Request Models
class TimeRange(BaseModel):
    start: datetime
    end: datetime

class TimeSeriesQuery(BaseModel):
    metric: str = Field(..., description="Metrica da query (visits, unique_users, etc)")
    start_time: datetime
    end_time: datetime  
    granularity: str = Field("hour", description="Granularità: minute, hour, day, month")
    filters: Optional[Dict[str, Any]] = {}

class AggregateQuery(BaseModel):
    metric: str
    dimensions: List[str] = Field(..., description="Dimensioni per group by")
    filters: Optional[Dict[str, Any]] = {}
    time_range: Optional[TimeRange] = None

class UserActivityQuery(BaseModel):
    user_id: int
    start_date: Optional[date] = None
    end_date: Optional[date] = None

class ShopPerformanceQuery(BaseModel):
    shop_ids: List[str]
    period_days: int = Field(7, description="Periodo analisi in giorni")

# Response Models
class TimeSeriesDataPoint(BaseModel):
    timestamp: datetime
    value: float
    metadata: Optional[Dict[str, Any]] = {}

class TimeSeriesResponse(BaseModel):
    data: List[TimeSeriesDataPoint]
    source: str = Field(..., description="stream o batch")
    cached: bool = False

class AggregateDataPoint(BaseModel):
    dimensions: Dict[str, Any]
    value: float
    count: Optional[int] = None

class AggregateResponse(BaseModel):
    data: List[AggregateDataPoint]
    dimensions: List[str]

class UserRealtimeActivity(BaseModel):
    last_position: Optional[Dict[str, float]]
    recent_shops: List[str]
    active_minutes: int
    messages_received: int

class UserHistoricalSummary(BaseModel):
    total_days_active: int
    total_shops_visited: int
    total_distance_km: float
    favorite_shops: List[Dict[str, Any]]
    peak_activity_hour: int

class UserActivityResponse(BaseModel):
    user_id: int
    realtime_activity: UserRealtimeActivity
    historical_summary: UserHistoricalSummary

class ShopMetrics(BaseModel):
    shop_id: str
    total_visits: int
    unique_visitors: int
    conversion_rate: float
    peak_hour: int
    avg_distance_m: float

class ShopTrend(BaseModel):
    shop_id: str
    trend_direction: str  # up, down, stable
    percent_change: float
    forecast_next_period: Optional[int]

class ShopPerformanceResponse(BaseModel):
    shops: List[ShopMetrics]
    trends: List[ShopTrend]
    period_days: int