from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
import aioredis
import logging
from fastapi_limiter import FastAPILimiter
from api.Asyncrq import asyncrq
from api.db_model import create_db_and_tables, add_default_values
from api.endpoints import auth, predict, users, documents
from .s3 import s3, BUCKET_NAME
from .env_config import config


app = FastAPI(title="docs-class")
app.include_router(auth.api_router, prefix="/v1/auth", tags=["auth"])
app.include_router(documents.router, prefix="/v1/documents", tags=["docs"])
app.include_router(predict.router, prefix="/v1/predict", tags=["pred"])
app.include_router(users.router, prefix="/v1/users", tags=["users"])


log = logging.getLogger("docs-class")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    global redis
    # redis = await aioredis.from_url(url="redis://redis")
    redis = await aioredis.from_url(url=f'redis://{config.redis_address}')
    await FastAPILimiter.init(redis)
    await create_db_and_tables()
    await add_default_values()
    await asyncrq.create_pool()
    print(s3)
    try:
        await s3.create_bucket(BUCKET_NAME)
    except Exception as e:
        print(e)
        # return json.dumps({"data": doc_id, "result": str(e)})



@app.on_event("shutdown")
async def shutdown_event():
    await redis.close()
