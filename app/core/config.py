import os
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import Dict, List, Optional

load_dotenv()

class Settings(BaseModel):
    """
    Application Configuration
    """
    DATA_GOV_API_KEY: Optional[str] = os.getenv("DATA_GOV_API_KEY")
    CLIENT_API_KEY: Optional[str] = os.getenv("CLIENT_API_KEY")
    UPSTASH_REDIS_REST_URL: Optional[str] = os.getenv("UPSTASH_REDIS_REST_URL")
    UPSTASH_REDIS_REST_TOKEN: Optional[str] = os.getenv("UPSTASH_REDIS_REST_TOKEN")
    NODE_ENV: str = os.getenv("NODE_ENV", "development")
    
    # Resources mapping
    RESOURCES: Dict[str, str] = {
        "enrolment": "ecd49b12-3084-4521-8f7e-ca8bf72069ba",
        "demographic": "19eac040-0b94-49fa-b239-4f2fd8677d53",
        "biometric": "65454dab-1517-40a3-ac1d-47d4dfe6891c",
    }
    
    def validate_keys(self):
        missing = []
        if not self.DATA_GOV_API_KEY: missing.append("DATA_GOV_API_KEY")
        if not self.CLIENT_API_KEY: missing.append("CLIENT_API_KEY")
        if not self.UPSTASH_REDIS_REST_URL: missing.append("UPSTASH_REDIS_REST_URL")
        if not self.UPSTASH_REDIS_REST_TOKEN: missing.append("UPSTASH_REDIS_REST_TOKEN")
        
        if missing and self.NODE_ENV != "development":
           # Log warning instead of crashing in serverless environment sometimes
           print(f"CRITICAL WARNING: Missing env vars: {', '.join(missing)}")

settings = Settings()
