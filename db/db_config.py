import json
import os

class DatabaseConfig:
    def __init__(self):
        # Get the directory of the current file (db_config.py)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Construct the path to config.json
        config_file_path = os.path.join(current_dir, '../config.json')

        # Load configuration from JSON file
        with open(config_file_path, 'r') as config_file:
            config = json.load(config_file)

        # Set DB connection details from config
        db_config = config["db_connection"]
        self.db_host = db_config["host"]
        self.db_name = db_config["db"]
        self.db_user = db_config["user"]
        self.db_password = db_config["password"]
        self.db_port = db_config["port"]
        self.db_driver = db_config.get("driver", "org.postgresql.Driver")  # Added default driver

        # Define the JDBC URL
        self.jdbc_url = f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_name}"

        # Define connection properties
        self.connection_properties = {
            "user": self.db_user,
            "password": self.db_password,
            "driver": self.db_driver
        }

    def get_jdbc_url(self):
        return self.jdbc_url

    def get_connection_properties(self):
        return self.connection_properties
