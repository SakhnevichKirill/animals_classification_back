import os
from dotenv import load_dotenv

class EnvConfig:
    def __init__(self):
        load_dotenv()
        self.redis_address = os.environ.get("redis_address")
        self.database_address = os.environ.get("database_address")
        self.s3_address = os.environ.get("s3_address")
    
    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

config = EnvConfig()
print(config)