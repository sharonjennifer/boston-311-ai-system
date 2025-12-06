from dotenv import load_dotenv
import os

load_dotenv()

class Settings:
    PROJECT_ID = os.getenv("B311_PROJECT_ID")
    EMBEDDING_MODEL = os.getenv("B311_EMBEDDING_MODEL")
    VERTEX_LOCATION = os.getenv("B311_VERTEX_LOCATION", "us-central1")
    INDEX_FILENAME = os.getenv("B311_INDEX_FILENAME")
    META_FILENAME = os.getenv("B311_META_FILENAME")

settings = Settings()
