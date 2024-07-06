import json
import uuid
import arq.jobs
from fastapi import Depends, APIRouter, HTTPException, status
from sqlalchemy import select
from api.db_model import User, TransactionHistory, get_session, MLModel
from api.endpoints.auth.FastAPI_users import current_active_user
from api.endpoints.auth.manuspect_users import auth_manuspect_user
from api.endpoints.predict.utils import validate_model_name
from sqlalchemy.ext.asyncio import AsyncSession
from api.Asyncrq import asyncrq
from fastapi_limiter.depends import RateLimiter
from worker.data_models.elderly_people import DataModel
from arq.jobs import Job
import arq

router = APIRouter(
    dependencies=[Depends(RateLimiter(times=15, seconds=5))],
)


# @router.get(
#     "/",
#     status_code=status.HTTP_200_OK,
# )
# async def get_models_list(
#     auth_token: str,
#     session: AsyncSession = Depends(get_session),
# ):
#     user: User = await auth_manuspect_user(auth_token, session)
#     results = await session.execute(select(MLModel))
#     models = results.scalars().all()
#     return [model.__dict__ for model in models]


@router.get(
    "/{job_id}",
    status_code=status.HTTP_200_OK,
)
async def get_job_result(
    auth_token: str,
    job_id: str,
    session: AsyncSession = Depends(get_session),
):
    user: User = await auth_manuspect_user(auth_token, session)
    job = Job(job_id=job_id, redis=asyncrq.pool)
    res = await job.result(timeout=30)
    return res 

@router.post(
    "/transaction/{job_id}",
    status_code=status.HTTP_200_OK,
)
async def update_transaction_result(
    job_id: str,
    json_data: str,
    auth_token: str,
    session: AsyncSession = Depends(get_session),
):
    user: User = await auth_manuspect_user(auth_token, session)

    transaction = await session.execute(
        select(TransactionHistory).filter_by(job_id=job_id)
    )
    transaction = transaction.scalar()
    transaction.result = json_data
    await session.commit()

@router.get(
    "/transaction/{job_id}",
    status_code=status.HTTP_200_OK,
)
async def get_transaction_result(
    job_id: str,
    auth_token: str,
    session: AsyncSession = Depends(get_session),
):
    user: User = await auth_manuspect_user(auth_token, session)

    transaction = await session.execute(
        select(TransactionHistory).filter_by(job_id=job_id).limit(1)
    )
    transaction = transaction.scalars().first()
    await transaction


# @router.post(
#     "/{model_name}",
#     status_code=status.HTTP_201_CREATED,
#     response_description="The task has been successfully created. Please track the execution by job_id",
# )
# async def get_predictions_for_data(
#     data: DataModel,
#     user: User = Depends(current_active_user),
#     session: AsyncSession = Depends(get_session),
# ):
#     result = await session.execute(select(MLModel).filter_by(model_name=model_name))
#     model = result.scalar()
#     if model is None:
#         raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Model not found")


#     transaction = TransactionHistory(
#         job_id=uuid.uuid4(),
#         user_id=user.id,
#         amount=-1 * model.model_cost,
#         model_id=model.id,
#     )
#     session.add(transaction)
#     await session.commit()
#     job: arq.jobs.Job = await asyncrq.pool.enqueue_job(
#         function="analyze_document",
#         _job_id=str(transaction.job_id),
#         model_name=model_name,
#         data=data.model_dump(),
#     )
    
#     info = await job.info()
#     return {
#         "job_id": str(job.job_id),
#         "job_try": str(info.job_try),
#         "enqueue_time": str(info.enqueue_time),
#         "remaining_balance": user.balance + transaction.amount,
#         "amount_spent": transaction.amount,
#     }
