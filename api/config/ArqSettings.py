from arq.connections import RedisSettings
from ..env_config import config

# arqsettings = RedisSettings(host="redis", port=6379, database=1)
arqsettings = RedisSettings(host=config.redis_address, port=6379, database=1)
