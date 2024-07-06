from api.config.ArqSettings import arqsettings
from worker.models_worker import analyze_document


class WorkerSettings:
    functions = [analyze_document]
    redis_settings = arqsettings
