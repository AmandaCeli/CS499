# animal_python.py
# CRUD module for CS-340
# Provides: AnimalShelter with create/read/update/delete + resilient connection.

from typing import Any, Dict, List, Optional
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError, OperationFailure


class AnimalShelter:
    """
    CRUD operations for the AAC 'animals' collection in MongoDB.

    create(data) -> bool
    read(query=None, projection=None, limit=None) -> List[dict]
    update(query, new_values, many=False) -> int
    delete(query, many=False) -> int
    ping() -> bool
    """

    def __init__(
        self,
        username: Optional[str] = "aacuser",
        password: Optional[str] = None,      # None => connect without auth (if server allows)
        host: str = "localhost",
        port: int = 27017,
        db_name: str = "AAC",                 # dataset DB (common seeds use 'AAC'; some use 'aac')
        collection_name: str = "animals",
        app_name: str = "CS340",
        auth_source: Optional[str] = None,    # where the user is defined; if None, auto-detect
        uri: Optional[str] = None,            # full URI override
    ) -> None:
        """
        Connection strategy:
          1) Use explicit URI if provided.
          2) Else try username/password across likely authSources: [auth_source|db_name, 'admin', 'AAC', 'aac'].
          3) Else try no-auth (for local mongod with auth disabled).
        Then select data DB/collection, falling back between 'AAC' and 'aac' if needed.
        """
        self.client: Optional[MongoClient] = None
        last_err: Optional[Exception] = None

        # 1) Explicit URI
        if uri:
            try:
                self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
                self.client.admin.command("ping")
            except Exception as e:
                last_err = e
                self.client = None

        # 2) Username/password with likely authSources
        if self.client is None and password:
            auth_candidates = []
            auth_candidates.append(auth_source or db_name)
            for cand in ("admin", "AAC", "aac"):
                if cand not in auth_candidates:
                    auth_candidates.append(cand)

            for auth_db in auth_candidates:
                try:
                    cred_uri = (
                        f"mongodb://{username}:{password}@{host}:{port}/"
                        f"?authSource={auth_db}&appName={app_name}"
                    )
                    self.client = MongoClient(cred_uri, serverSelectionTimeoutMS=5000)
                    self.client.admin.command("ping")
                    break
                except Exception as e:
                    last_err = e
                    self.client = None

        # 3) No-auth (if allowed)
        if self.client is None:
            try:
                noauth_uri = f"mongodb://{host}:{port}/?appName={app_name}"
                self.client = MongoClient(noauth_uri, serverSelectionTimeoutMS=5000)
                self.client.admin.command("ping")
            except Exception as e:
                last_err = e
                self.client = None

        if self.client is None:
            raise RuntimeError(f"Failed to connect to MongoDB: {last_err}")

        # Choose data DB/collection; fallback AAC<->aac if empty/unavailable
        try:
            self.database = self.client[db_name]
            self.collection: Collection = self.database[collection_name]

            try:
                empty = (self.collection.estimated_document_count() == 0)
            except OperationFailure:
                # If no privilege for count, try reading one doc
                empty = (self.collection.find_one({}) is None)

            if empty:
                for candidate in ("AAC", "aac"):
                    if candidate != db_name and candidate in self.client.list_database_names():
                        alt_col = self.client[candidate][collection_name]
                        try:
                            if alt_col.estimated_document_count() > 0:
                                self.database = self.client[candidate]
                                self.collection = alt_col
                                break
                        except OperationFailure:
                            if self.client[candidate][collection_name].find_one({}):
                                self.database = self.client[candidate]
                                self.collection = self.client[candidate][collection_name]
                                break
        except PyMongoError as e:
            raise RuntimeError(f"Connected but failed to select collection: {e}") from e

    # -------------------- CREATE --------------------
    def create(self, data: Dict[str, Any]) -> bool:
        if not isinstance(data, dict) or not data:
            return False
        try:
            res = self.collection.insert_one(data)
            return bool(res.acknowledged)
        except PyMongoError:
            return False

    # --------------------- READ ---------------------
    def read(
        self,
        query: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        try:
            cur = self.collection.find(query or {}, projection or {})
            if isinstance(limit, int) and limit > 0:
                cur = cur.limit(limit)
            return list(cur)
        except PyMongoError:
            return []

    # -------------------- UPDATE --------------------
    def update(self, query: Dict[str, Any], new_values: Dict[str, Any], many: bool = False) -> int:
        if not query or not new_values:
            return 0
        try:
            fn = self.collection.update_many if many else self.collection.update_one
            res = fn(query, {"$set": new_values})
            return int(res.modified_count)
        except PyMongoError:
            return 0

    # -------------------- DELETE --------------------
    def delete(self, query: Dict[str, Any], many: bool = False) -> int:
        if not query:
            return 0
        try:
            fn = self.collection.delete_many if many else self.collection.delete_one
            res = fn(query)
            return int(res.deleted_count)
        except PyMongoError:
            return 0

    # -------------------- HEALTHCHECK --------------------
    def ping(self) -> bool:
        try:
            self.client.admin.command("ping")
            return True
        except PyMongoError:
            return False
