import uuid
import arq.jobs
from fastapi import Depends, APIRouter, HTTPException, Response, status,  UploadFile, File
from sqlalchemy import select
from api.db_model import User, TransactionHistory, get_session, MLModel, Document, UsersToDocuments
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
#     document: str,
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
        select(Document)
        .join(UsersToDocuments, UsersToDocuments.document_id == Document.id)
        .filter(UsersToDocuments.user_id == user.id, Document.id == filename, Document.is_deleted == False)
    )
    document = result.scalars().first()
    if not document:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")
    data = await s3.download_file(filename)
    data = ""
    headers = {
        'Content-Disposition': f'attachment; filename="{document.name}"'
    }
    return Response(content=data, media_type="application/octet-stream", headers=headers)

@router.delete(
    "/{document_id}",
    status_code=status.HTTP_200_OK
)
async def delete_document(
    auth_token: str,
    document_id: str,
    session: AsyncSession = Depends(get_session)
):
    user: User = await auth_manuspect_user(auth_token, session)
    # Fetch the document to ensure it exists and is associated with the user
    result = await session.execute(
        select(Document)
        .join(UsersToDocuments, UsersToDocuments.document_id == Document.id)
        .filter(
            UsersToDocuments.user_id == user.id,
            Document.id == document_id,
            Document.is_deleted == False
        )
    )
    document = result.scalars().first()
    
    # If the document is not found or already deleted, return a 404 error
    if not document:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document not found"
        )
    
    # Mark the document as deleted
    document.is_deleted = True
    session.add(document)
    await session.commit()
    
    return {"detail": "Document deleted successfully"}

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
    documents_stmt = select(Document).\
        join(UsersToDocuments, Document.id == UsersToDocuments.document_id).\
        filter(UsersToDocuments.user_id == user.id)

    # Выполнение запроса для получения документов
    document_results = await session.execute(documents_stmt)
    documents = document_results.scalars().all()

    if not documents:
        return []

    # Собираем идентификаторы документов
    document_ids = [document.id for document in documents]

    # Запрос для получения job_id для этих документов
    transactions_stmt = select(TransactionHistory.document_id, TransactionHistory.job_id).\
        filter(TransactionHistory.document_id.in_(document_ids))

    # Выполнение запроса для получения транзакций
    transaction_results = await session.execute(transactions_stmt)
    transactions = transaction_results.all()

    # Создаем словарь для эффективного поиска job_id по document_id
    transaction_map = {doc_id: job_id for doc_id, job_id in transactions}

    # Собираем результат для возврата
    documents_with_jobs = [
        {
            **document.__dict__,
            "job_id": transaction_map.get(document.id)
        }
        for document in documents
    ]

    # Убираем служебное поле _sa_instance_state
    for document in documents_with_jobs:
        document.pop('_sa_instance_state', None)

    return documents_with_jobs

@router.get(
    "/status/{doc_id}",
    status_code=status.HTTP_200_OK,
)
async def get_docs_info_by_user(
    doc_id: str | None,
    auth_token: str,
    session: AsyncSession = Depends(get_session),
):
    user: User = await auth_manuspect_user(auth_token, session)

    # Создаем запрос с объединением таблиц документов и транзакций
    stmt = select(Document)\
        .join(UsersToDocuments, Document.id == UsersToDocuments.document_id)\
        .join(TransactionHistory, Document.id == TransactionHistory.document_id, isouter=True)\
        .filter(
            UsersToDocuments.user_id == user.id,
            Document.id == doc_id,  # Добавляем условие по doc_id
            Document.is_deleted == False,
        )\
        .options(joinedload(Document.transaction_history))\
        .limit(1)  # Ограничиваем количество результатов до одного

    # Выполняем запрос
    result = await session.execute(stmt)

    # Получаем один документ пользователя, если такой существует
    document = result.scalars().first()
    return document


@router.get(
    "/verify/{doc_id}",
    status_code=status.HTTP_200_OK,
)
async def verify_document(
    any_error_verified: bool,
    any_error_reason: str,
    doc_id: str | None,
    auth_token: str,
    session: AsyncSession = Depends(get_session),
):
    user: User = await auth_manuspect_user(auth_token, session)

    # Создаем запрос с объединением таблиц документов и транзакций
    stmt = select(Document)\
        .join(UsersToDocuments, Document.id == UsersToDocuments.document_id)\
        .join(TransactionHistory, Document.id == TransactionHistory.document_id, isouter=True)\
        .filter(
            UsersToDocuments.user_id == user.id,
            Document.id == doc_id,  # Добавляем условие по doc_id
            Document.is_deleted == False,
        )\
        .options(joinedload(Document.transaction_history))\
        .limit(1)  # Ограничиваем количество результатов до одного

    # Выполняем запрос
    result = await session.execute(stmt)

    # Получаем один документ пользователя, если такой существует
    document = result.scalars().first()

    if document:
        document.any_error_verified = any_error_verified
        document.any_error_reason = any_error_reason
        return document
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document not found"
        )



@router.post(
    "/", 
    status_code=status.HTTP_201_CREATED,
    response_description="The document has been successfully created. Please track the execution by job_id",
)
async def upload_document(
    auth_token: str,
    document: UploadFile = File(description="Your document (max 10 MB)"),
    session: AsyncSession = Depends(get_session),
):
    user: User = await auth_manuspect_user(auth_token, session)

    doc_id = uuid.uuid4()
    await s3.upload_file(file=document.file, filename=str(doc_id))

    new_document = Document(id=doc_id, name=document.filename)
    session.add(new_document)
    
    new_users_to_document = UsersToDocuments(user_id=user.id, document_id=new_document.id)
    session.add(new_users_to_document)    
    await session.commit()

    transaction = TransactionHistory(
        job_id=uuid.uuid4(),
        user_id=user.id,
        amount=0,
        document_id=new_document.id,
        model_id=None,
    )
    session.add(transaction)
    await session.commit()

    
    job = await asyncrq.pool.enqueue_job(
        function="analyze_document",
        _job_id=str(transaction.job_id),
        doc_id=str(doc_id),
        doc_name=document.filename
    )
    
    info = await job.info()
    
    return {
        "job_id": str(job.job_id),
        "job_try": str(info.job_try),
        "doc_id": str(doc_id),
        "enqueue_time": str(info.enqueue_time),
    }
