from fastapi import Depends, FastAPI, BackgroundTasks, HTTPException, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from typing import List, Dict, Any, Union
from pydantic import BaseModel
from sqlalchemy import and_
import requests
import time

# Create the database engine
engine = create_engine("postgresql://postgres:postgres@localhost/coinstats")
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create the base model
Base = declarative_base()

# Define the Coin model
class Coin(Base):
    __tablename__ = "coins"

    id = Column(String, primary_key=True)
    rank = Column(Integer)
    symbol = Column(String)
    name = Column(String)
    supply = Column(Float)
    max_supply = Column(Float)
    market_cap_usd = Column(Float)
    volume_usd_24h = Column(Float)
    price_usd = Column(Float)
    change_percent_24hr = Column(Float)
    vwap_24hr = Column(Float)
    explorer = Column(String)

# Define the PriceLog model
class PriceLog(Base):
    __tablename__ = "price_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    coin_id = Column(String)
    coin_symbol = Column(String)  # New column
    price_usd = Column(Float)
    log_time = Column(DateTime, default=datetime.now)


# Create the tables
Base.metadata.create_all(bind=engine)

# Create the FastAPI app
app = FastAPI()

# Define a static token for validation
STATIC_TOKEN = "token"

# Create an instance of HTTPBearer for token authentication
security = HTTPBearer()

# Function to validate the token
def validate_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials

    if token != STATIC_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")

# Function to fetch and save coin data and price log
def fetch_and_save_coins():
    while True:
        # Fetch data from CoinCap API
        response = requests.get("https://api.coincap.io/v2/assets")
        data = response.json()["data"]

        # Create a session
        session = SessionLocal()

        # Save the data and log the time for each coin
        for coin in data:
            max_supply = float(coin["maxSupply"]) if coin["maxSupply"] is not None else None
            vwap_24hr = float(coin["vwap24Hr"]) if coin["vwap24Hr"] is not None else None

            # Check if the coin already exists in the database
            existing_coin = session.query(Coin).filter_by(id=coin["id"]).first()

            if existing_coin:
                # Update the existing coin data
                existing_coin.rank = coin["rank"]
                existing_coin.symbol = coin["symbol"].lower()
                existing_coin.name = coin["name"].lower()
                existing_coin.supply = float(coin["supply"])
                existing_coin.max_supply = max_supply
                existing_coin.market_cap_usd = float(coin["marketCapUsd"])
                existing_coin.volume_usd_24h = float(coin["volumeUsd24Hr"])
                existing_coin.price_usd = float(coin["priceUsd"])
                existing_coin.change_percent_24hr = float(coin["changePercent24Hr"])
                existing_coin.vwap_24hr = vwap_24hr
                existing_coin.explorer = coin["explorer"]
            else:
                # Create a new coin entry
                coin_data = Coin(
                    id=coin["id"],
                    rank=coin["rank"],
                    symbol=coin["symbol"].lower(),
                    name=coin["name"].lower(),
                    supply=float(coin["supply"]),
                    max_supply=max_supply,
                    market_cap_usd=float(coin["marketCapUsd"]),
                    volume_usd_24h=float(coin["volumeUsd24Hr"]),
                    price_usd=float(coin["priceUsd"]),
                    change_percent_24hr=float(coin["changePercent24Hr"]),
                    vwap_24hr=vwap_24hr,
                    explorer=coin["explorer"]
                )
                session.add(coin_data)

            # Create a new price log entry
            now = datetime.now()
            # Extract the day and hour components
            day = now.day
            hour = now.hour
            # Create a new price log entry
            price_log = PriceLog(
                coin_id=coin["id"],
                coin_symbol=coin["symbol"].lower(),  # Save coin symbol
                price_usd=float(coin["priceUsd"]),
                log_time=datetime(year=now.year, month=now.month, day=day, hour=hour)
                )
            session.add(price_log)

                # Commit the changes to the database
            session.commit()
            session.close()

        # Wait for 1 hour before fetching the data again
        time.sleep(3600)

# Register an event handler for when the application starts
@app.on_event("startup")
async def startup_event():
    # Create a background task to fetch and save coins
    background_tasks = BackgroundTasks()
    background_tasks.add_task(fetch_and_save_coins)
    app.background_tasks = background_tasks

# Endpoint to start the background task
@app.get("/start-task")
async def start_task(background_tasks: BackgroundTasks, credentials: HTTPAuthorizationCredentials = Depends(validate_token)):
    background_tasks.add_task(fetch_and_save_coins)
    return {"message": "Background task started."}


# Data model for price log response
class PriceLogData(BaseModel):
    price: float
    log_time: datetime


## Endpoint to get all price logs and coin data for all coins
@app.get("/coins/price-logs")
def get_all_coin_price_logs(start_date: datetime = Query(None, description="Start date for the log"), end_date: datetime = Query(None, description="End date for the log")) -> List[Dict[str, Any]]:
    session = SessionLocal()

    coins = session.query(Coin).all()

    coin_data = []

    for coin in coins:
        query = session.query(PriceLog).filter(PriceLog.coin_id == coin.id)
        if start_date and end_date:
            query = query.filter(PriceLog.log_time >= start_date, PriceLog.log_time <= end_date)
        elif start_date:
            query = query.filter(PriceLog.log_time >= start_date)
        elif end_date:
            query = query.filter(PriceLog.log_time <= end_date)
        price_logs = query.all()

        coin_logs = [
            PriceLogData(price=price_log.price_usd, log_time=price_log.log_time)
            for price_log in price_logs
        ]

        coin_info = {
            "id": coin.id,
            "rank": coin.rank,
            "symbol": coin.symbol,
            "name": coin.name,
            "supply": coin.supply,
            "max_supply": coin.max_supply,
            "market_cap_usd": coin.market_cap_usd,
            "volume_usd_24h": coin.volume_usd_24h,
            "price_usd": coin.price_usd,
            "change_percent_24hr": coin.change_percent_24hr,
            "vwap_24hr": coin.vwap_24hr,
            "explorer": coin.explorer,
            "price_logs": coin_logs,
        }

        coin_data.append(coin_info)

    session.close()

    return coin_data


## Endpoint to get price logs and data for a specific coin by ID or symbol
@app.get("/coin/{coin_id}/price-logs")
def get_coin_price_logs(coin_id: str, start_date: datetime = Query(None, description="Start date for the log"), end_date: datetime = Query(None, description="End date for the log")) -> Dict[str, Any]:
    session = SessionLocal()

    # Check if coin_id is a valid ID
    coin = session.query(Coin).filter_by(id=coin_id).first()

    if not coin:
        # Check if coin_id is a valid symbol
        coin = session.query(Coin).filter_by(symbol=coin_id.lower()).first()

        if not coin:
            raise HTTPException(status_code=404, detail="Coin not found")

    query = session.query(PriceLog).filter(PriceLog.coin_id == coin.id)
    if start_date and end_date:
        query = query.filter(PriceLog.log_time >= start_date, PriceLog.log_time <= end_date)
    elif start_date:
        query = query.filter(PriceLog.log_time >= start_date)
    elif end_date:
        query = query.filter(PriceLog.log_time <= end_date)
    price_logs = query.all()

    coin_logs = [
        PriceLogData(price=price_log.price_usd, log_time=price_log.log_time)
        for price_log in price_logs
    ]

    coin_info = {
        "id": coin.id,
        "rank": coin.rank,
        "symbol": coin.symbol,
        "name": coin.name,
        "supply": coin.supply,
        "max_supply": coin.max_supply,
        "market_cap_usd": coin.market_cap_usd,
        "volume_usd_24h": coin.volume_usd_24h,
        "price_usd": coin.price_usd,
        "change_percent_24hr": coin.change_percent_24hr,
        "vwap_24hr": coin.vwap_24hr,
        "explorer": coin.explorer,
        "price_logs": coin_logs,
    }

    session.close()

    return coin_info

if __name__ == '__main__':
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level='info')
