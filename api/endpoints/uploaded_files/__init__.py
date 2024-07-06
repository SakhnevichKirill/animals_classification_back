import uuid
import arq.jobs
from fastapi import Depends, APIRouter, HTTPException, Response, status,  UploadFile, File
from sqlalchemy import select
from api.db_model import User, TransactionHistory, get_session, MLModel, UploadedFile, UsersToDocuments
from api.endpoints.auth.FastAPI_users import current_active_user
from api.endpoints.auth.manuspect_users import auth_manuspect_user
from api.endpoints.predict.utils import validate_model_name
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
from api.Asyncrq import asyncrq
from fastapi_limiter.depends import RateLimiter
from worker.data_models.elderly_people import DataModel
from arq.jobs import Job
import arq
from api.s3 import s3

router = APIRouter(
    dependencies=[Depends(RateLimiter(times=15, seconds=5))],
)


#TODO
# @router.get(
#     "/",
#     status_code=status.HTTP_200_OK,
# )
# async def get_doc(
#     uploaded_file: str,
#     target_class: str,
#     user: User = Depends(current_active_user),
#     session: AsyncSession = Depends(get_session),
    
# ):
#     results = await session.execute(select(MLModel))
#     models = results.scalars().all()
#     return [model.__dict__ for model in models]


@router.get(
    "/{filename}",
    status_code=status.HTTP_200_OK,
)
async def download_docs(
    auth_token: str,
    filename: str,
    session: AsyncSession = Depends(get_session),
):
    user: User = await auth_manuspect_user(auth_token, session)
    # Проверка доступа к файлу
    result = await session.execute(
        select(UploadedFile)
        .join(UsersToDocuments, UsersToDocuments.uploaded_file_id == UploadedFile.id)
        .filter(UsersToDocuments.user_id == user.id, UploadedFile.id == filename, UploadedFile.is_deleted == False)
    )
    uploaded_file = result.scalars().first()
    if not uploaded_file:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")
    data = await s3.download_file(filename)
    data = ""
    headers = {
        'Content-Disposition': f'attachment; filename="{uploaded_file.name}"'
    }
    return Response(content=data, media_type="application/octet-stream", headers=headers)

@router.delete(
    "/{uploaded_file_id}",
    status_code=status.HTTP_200_OK
)
async def delete_uploaded_file(
    auth_token: str,
    uploaded_file_id: str,
    session: AsyncSession = Depends(get_session)
):
    user: User = await auth_manuspect_user(auth_token, session)
    # Fetch the uploaded_file to ensure it exists and is associated with the user
    result = await session.execute(
        select(UploadedFile)
        .join(UsersToDocuments, UsersToDocuments.uploaded_file_id == UploadedFile.id)
        .filter(
            UsersToDocuments.user_id == user.id,
            UploadedFile.id == uploaded_file_id,
            UploadedFile.is_deleted == False
        )
    )
    uploaded_file = result.scalars().first()
    
    # If the uploaded_file is not found or already deleted, return a 404 error
    if not uploaded_file:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="UploadedFile not found"
        )
    
    # Mark the uploaded_file as deleted
    uploaded_file.is_deleted = True
    session.add(uploaded_file)
    await session.commit()
    
    return {"detail": "UploadedFile deleted successfully"}

from sqlalchemy.orm import aliased
from sqlalchemy.orm import contains_eager

th_alias = aliased(TransactionHistory)

@router.get(
    "/",
    status_code=status.HTTP_200_OK,
)
async def get_docs_info_by_user(
    auth_token: str,
    session: AsyncSession = Depends(get_session),
):
    user: User = await auth_manuspect_user(auth_token, session)
    # Запрос для получения документов, связанных с пользователем
    uploaded_files_stmt = select(UploadedFile).\
        join(UsersToDocuments, UploadedFile.id == UsersToDocuments.uploaded_file_id).\
        filter(UsersToDocuments.user_id == user.id)

    # Выполнение запроса для получения документов
    uploaded_file_results = await session.execute(uploaded_files_stmt)
    uploaded_files = uploaded_file_results.scalars().all()

    if not uploaded_files:
        return []

    # Собираем идентификаторы документов
    uploaded_file_ids = [uploaded_file.id for uploaded_file in uploaded_files]

    # Запрос для получения job_id для этих документов
    transactions_stmt = select(TransactionHistory.uploaded_file_id, TransactionHistory.job_id).\
        filter(TransactionHistory.uploaded_file_id.in_(uploaded_file_ids))

    # Выполнение запроса для получения транзакций
    transaction_results = await session.execute(transactions_stmt)
    transactions = transaction_results.all()

    # Создаем словарь для эффективного поиска job_id по uploaded_file_id
    transaction_map = {uploaded_file_id: job_id for uploaded_file_id, job_id in transactions}

    # Собираем результат для возврата
    uploaded_files_with_jobs = [
        {
            **uploaded_file.__dict__,
            "job_id": transaction_map.get(uploaded_file.id)
        }
        for uploaded_file in uploaded_files
    ]

    # Убираем служебное поле _sa_instance_state
    for uploaded_file in uploaded_files_with_jobs:
        uploaded_file.pop('_sa_instance_state', None)

    return uploaded_files_with_jobs

@router.get(
    "/status/{uploaded_file_id}",
    status_code=status.HTTP_200_OK,
)
async def get_docs_info_by_user(
    uploaded_file_id: str | None,
    auth_token: str,
    session: AsyncSession = Depends(get_session),
):
    user: User = await auth_manuspect_user(auth_token, session)

    # Создаем запрос с объединением таблиц документов и транзакций
    stmt = select(UploadedFile)\
        .join(UsersToDocuments, UploadedFile.id == UsersToDocuments.uploaded_file_id)\
        .join(TransactionHistory, UploadedFile.id == TransactionHistory.uploaded_file_id, isouter=True)\
        .filter(
            UsersToDocuments.user_id == user.id,
            UploadedFile.id == uploaded_file_id,  # Добавляем условие по uploaded_file_id
            UploadedFile.is_deleted == False,
        )\
        .options(joinedload(UploadedFile.transaction_history))\
        .limit(1)  # Ограничиваем количество результатов до одного

    # Выполняем запрос
    result = await session.execute(stmt)

    # Получаем один документ пользователя, если такой существует
    uploaded_file = result.scalars().first()
    return uploaded_file


@router.get(
    "/verify/{uploaded_file_id}",
    status_code=status.HTTP_200_OK,
)
async def verify_uploaded_file(
    any_error_verified: bool,
    any_error_reason: str,
    uploaded_file_id: str | None,
    auth_token: str,
    session: AsyncSession = Depends(get_session),
):
    user: User = await auth_manuspect_user(auth_token, session)

    # Создаем запрос с объединением таблиц документов и транзакций
    stmt = select(UploadedFile)\
        .join(UsersToDocuments, UploadedFile.id == UsersToDocuments.uploaded_file_id)\
        .join(TransactionHistory, UploadedFile.id == TransactionHistory.uploaded_file_id, isouter=True)\
        .filter(
            UsersToDocuments.user_id == user.id,
            UploadedFile.id == uploaded_file_id,  # Добавляем условие по uploaded_file_id
            UploadedFile.is_deleted == False,
        )\
        .options(joinedload(UploadedFile.transaction_history))\
        .limit(1)  # Ограничиваем количество результатов до одного

    # Выполняем запрос
    result = await session.execute(stmt)

    # Получаем один документ пользователя, если такой существует
    uploaded_file = result.scalars().first()

    if uploaded_file:
        uploaded_file.any_error_verified = any_error_verified
        uploaded_file.any_error_reason = any_error_reason
        return uploaded_file
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="UploadedFile not found"
        )



@router.post(
    "/", 
    status_code=status.HTTP_201_CREATED,
    response_description="The uploaded_file has been successfully created. Please track the execution by job_id",
)
async def upload_uploaded_file(
    auth_token: str,
    uploaded_file: UploadFile = File(description="Your uploaded_file (max 10 MB)"),
    session: AsyncSession = Depends(get_session),
):
    user: User = await auth_manuspect_user(auth_token, session)

    uploaded_file_id = uuid.uuid4()
    await s3.upload_file(file=uploaded_file.file, filename=str(uploaded_file_id))

    new_uploaded_file = UploadedFile(id=uploaded_file_id, name=uploaded_file.filename)
    session.add(new_uploaded_file)
    
    new_users_to_uploaded_file = UsersToDocuments(user_id=user.id, uploaded_file_id=new_uploaded_file.id)
    session.add(new_users_to_uploaded_file)    
    await session.commit()

    transaction = TransactionHistory(
        job_id=uuid.uuid4(),
        user_id=user.id,
        amount=0,
        uploaded_file_id=new_uploaded_file.id,
        model_id=None,
    )
    session.add(transaction)
    await session.commit()

    
    job = await asyncrq.pool.enqueue_job(
        function="analyze_uploaded_file",
        _job_id=str(transaction.job_id),
        uploaded_file_id=str(uploaded_file_id),
        uploaded_file_name=uploaded_file.filename
    )
    
    info = await job.info()
    
    return {
        "job_id": str(job.job_id),
        "job_try": str(info.job_try),
        "uploaded_file_id": str(uploaded_file_id),
        "enqueue_time": str(info.enqueue_time),
    }
