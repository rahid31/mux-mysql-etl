# API Request ETL to MySQL Database

This Python script extracts MUX data from an API, transforms it into a structured format, and loads it into a MySQL database. The project is designed to handle large data loads efficiently and supports authentication and time-based filtering for API requests.

---

## Features

- **Extract:** Fetches data from an API using HTTP Basic Authentication.
- **Transform:** Formats and processes the data into a structured pandas DataFrame.
- **Load:** Inserts the transformed data into a MySQL database table.
- **Configurable:** Leverages environment variables for secure and flexible configuration.

---

## Requirements

- Python 3.12+
- MySQL database
- `.env` file to manage credentials and configurations securely.
- A [MUX](https://mux.com/) account with access to the desired API.

---

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/rahid31/mux-mysql-etl.git
   cd mux-mysql-etl
