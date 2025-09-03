import os
import time
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure
import logging

class MongoManager:
    def __init__(self, connection_string=None, db_name="hls_streamer"):
        self.db_name = db_name
        # Prioritize environment variable, then provided string, then default localhost
        self.connection_string = connection_string or os.environ.get("MONGO_URI", "mongodb://localhost:27017")
        self.client = None
        self.db = None
        self.sessions = None

    async def connect(self):
        """Establishes connection to the database."""
        try:
            self.client = AsyncIOMotorClient(self.connection_string)
            # The ismaster command is cheap and does not require auth.
            await self.client.admin.command('ismaster')
            self.db = self.client[self.db_name]
            self.sessions = self.db.sessions
            logging.info("Successfully connected to MongoDB.")
        except ConnectionFailure as e:
            logging.critical(f"Could not connect to MongoDB: {e}")
            raise

    async def close(self):
        """Closes the database connection."""
        if self.client:
            self.client.close()
            logging.info("MongoDB connection closed.")

    async def create_session(self, session_id: str, context: dict, pid: int = None):
        """Creates a new stream session."""
        doc = {
            "_id": session_id,
            "stream_context": context,
            "pid": pid,
            "process_running": pid is not None, # Initially true if PID is provided
            "last_seen": time.time(),
            "status_message": "Initializing...",
            "progress_data": {},
            "created_at": time.time()
        }
        await self.sessions.insert_one(doc)
        return doc

    async def get_session(self, session_id: str):
        """Retrieves a session by its ID."""
        return await self.sessions.find_one({"_id": session_id})

    async def update_session(self, session_id: str, updates: dict):
        """Updates a session document."""
        return await self.sessions.update_one({"_id": session_id}, {"$set": updates})

    async def delete_session(self, session_id: str):
        """Deletes a session by its ID."""
        return await self.sessions.delete_one({"_id": session_id})

    async def get_all_sessions(self):
        """Retrieves all active sessions."""
        cursor = self.sessions.find({}) # Find all documents
        return await cursor.to_list(length=None) # Use length=None to get all documents

    async def get_timed_out_sessions(self, timeout_seconds: int):
        """Finds sessions that have not had a heartbeat in time."""
        cutoff_time = time.time() - timeout_seconds
        cursor = self.sessions.find({
            "process_running": True, # Only consider processes that are marked as running
            "last_seen": {"$lt": cutoff_time}
        })
        return await cursor.to_list(length=None)

    async def find_session_by_magnet(self, magnet_url: str, exclude_session_id: str = None):
        """Finds a session by magnet URL, optionally excluding a specific session ID."""
        query = {
            "stream_context.magnet_url": magnet_url
        }
        if exclude_session_id:
            query["_id"] = {"$ne": exclude_session_id}
        return await self.sessions.find_one(query)

    async def get_running_stream_count(self):
        """Counts the number of currently running streams."""
        return await self.sessions.count_documents({"process_running": True})

    async def set_process_running_false(self, session_id: str):
        """Mark a process as not running, e.g., when it has finished."""
        return await self.update_session(session_id, {
            "process_running": False,
            "last_seen": time.time() # Update last_seen to give it time before cleanup
        })
