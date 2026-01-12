import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    DATA_GOV_API_KEY = os.getenv("DATA_GOV_API_KEY")
    CLIENT_API_KEY = os.getenv("CLIENT_API_KEY")
    UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
    UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")
    NODE_ENV = os.getenv("NODE_ENV", "development")
    
    # Resources mapping
    RESOURCES = {
        "enrolment": "ecd49b12-3084-4521-8f7e-ca8bf72069ba",
        "demographic": "19eac040-0b94-49fa-b239-4f2fd8677d53",
        "biometric": "65454dab-1517-40a3-ac1d-47d4dfe6891c",
    }
    
    def validate(self):
        missing = []
        if not self.DATA_GOV_API_KEY: missing.append("DATA_GOV_API_KEY")
        if not self.CLIENT_API_KEY: missing.append("CLIENT_API_KEY")
        if not self.UPSTASH_REDIS_REST_URL: missing.append("UPSTASH_REDIS_REST_URL")
        if not self.UPSTASH_REDIS_REST_TOKEN: missing.append("UPSTASH_REDIS_REST_TOKEN")
        
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

config = Settings()
config.validate()
