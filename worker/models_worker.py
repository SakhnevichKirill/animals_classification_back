import json
from typing import Dict, Any

from sqlalchemy import select
# from worker.models.text_classification_hackaton import Model
from api.db_model import TransactionHistory, get_session, TransactionStatusEnum, UploadedFile
from sqlalchemy.exc import NoResultFound
# from worker.models.text_classification_hackaton.utils import FileProcessor
from api.s3 import s3
import traceback

# model = Model()
# file_proc = FileProcessor()


async def analyze_uploaded_file(
    ctx: Dict[str, Any],
    uploaded_file_id : str,
    uploaded_file_name: str,
    **kwargs: Any,
):
    async for session in get_session():
        try:
            
            job_id = ctx.get("job_id", None)
            if not job_id:
                raise Exception("Something is wrong. job_id is None")

            transaction = await session.execute(
                select(TransactionHistory).filter_by(job_id=job_id)
            )
            transaction = transaction.scalar()
            
            doc = await session.execute(
                select(UploadedFile).filter_by(id=uploaded_file_id)
            )
            doc = doc.scalar()
            
            # data = await file_proc.process_file(uploaded_file_name, uploaded_file_id)
            
            # result = model.predict(data)
            result = "Test results"
            if not result:
                raise Exception(f"Something is wrong. Try again later: {result}")
            
            if(result != class_name):
                doc.verified = False
                doc.cancellation_reason = f"Model got {result} class but {class_name} was expected"
            else:
                doc.verified = True

            transaction.status = TransactionStatusEnum.SUCCESS

            json_data = json.dumps({"data": uploaded_file_id, "result": f"Model got {result} class ({class_name} expected)"})
            transaction.result = json_data
            await session.commit()
            return result
        except Exception as e:
            transaction.status = TransactionStatusEnum.FAILURE
            transaction.err_reason = str(e)
            doc.verified = False
            doc.cancellation_reason = f"File processing error: {e}"
            await session.commit()
            traceback.print_exc()
            return json.dumps({"data": uploaded_file_id, "result": str(e)})
