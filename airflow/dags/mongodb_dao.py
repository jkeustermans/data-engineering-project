from pymongo import MongoClient

class MongoDBDAO:

    def __init__(self, host, port, user, password):
        self.db_host = host
        self.db_port = port
        self.db_user = user
        self.db_password = password

    def determine_dwh_database_url(self):
        return f"mongodb://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}"

    def persist_dataframe_to_medical_documents_collection(self, dataframe):
        connection_string = self.determine_dwh_database_url()
        with MongoClient(connection_string) as client:
            db = client["pps"]
            collection_medical_documents = db["medical_documents"]
            records = dataframe.to_dict(orient="records")
            collection_medical_documents.insert_many(records)