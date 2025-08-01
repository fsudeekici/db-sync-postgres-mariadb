# Postgres to MariaDB Sync

This project transfers data between a PostgreSQL database and a MariaDB database. It is useful for moving customer and route information between two systems. The sync runs automatically every day at 23:00.

## Features

- Connects to PostgreSQL and MariaDB using environment variables
- Transfers selected tables (like customers and routes)
- Sends a Teams message after sync is complete
- Can run manually or on a daily schedule

## Technologies Used

- Python 3
- psycopg2 (PostgreSQL connection)
- mysql-connector-python (MariaDB connection)
- schedule (for running tasks)
- python-dotenv (for reading `.env` file)
- requests (for sending Teams messages)
