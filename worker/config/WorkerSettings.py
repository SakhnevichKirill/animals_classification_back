from api.config.ArqSettings import arqsettings
from worker.models_worker import analyze_uploaded_file


class WorkerSettings:
    functions = [analyze_uploaded_file]
    redis_settings = arqsettings
