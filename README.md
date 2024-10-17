
## Getting Started

Follow these steps to set up the project:

### Step 1: Update PostgreSQL Database Credentials

1. Open the `dbconfig.json` file located in the root directory of your project.
2. Update the credentials for the key `"db_connection"` with your PostgreSQL database details.:

   ```json
   {
       "db_connection": {
           "host": "localhost",
           "port": 5432,
           "username": "your_username",
           "password": "your_password",
           "database": "your_database"
       }
   }
3. Open the `docker-compose.yml` file located in the root directory of your project.
4. Update the credentials for the key `"environment"` with your PostgreSQL database details.:
   ```yaml
   environment:
   - POSTGRES_HOST=host.docker.internal  # Connect to the host's PostgreSQL database
   - POSTGRES_DB=your_database
   - POSTGRES_USER=your_username
   - POSTGRES_PASSWORD=your_password
   - POSTGRES_PORT=5432

### Step 2: Build Docker Container
    docker-compose build

### Step 3: Start Docker Container
    docker-compose up
