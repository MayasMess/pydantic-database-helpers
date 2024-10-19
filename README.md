# 1 - OracleHelper Module

A Python module that facilitates interactions with Oracle databases (can be extended to other db) using `Pydantic` for data validation and `SQLAlchemy` for database operations. This module provides a structured and type-safe way to perform common database operations such as inserts, updates, deletions, and selections.

## Features

- **Insert Records**: Supports inserting single records or multiple records into a specified table.
- **Upsert Records**: Allows for inserting or updating records based on specified unique fields.
- **Delete Records**: Supports deleting single or multiple records using specified conditions.
- **Update Records**: Allows updating records with specified conditions.
- **Select Records**: Fetches single or multiple records with optional `WHERE` clauses.
- **Select in Batches**: Retrieve data in chunks for better memory management with large datasets.
- **Connection Management**: Automatically handles connection and engine creation, with cleanup functionality to close connections safely.

## Installation (not packaged yet)

Make sure you have the `oracledb`, `pydantic`, and `sqlalchemy` packages installed:

```bash
pip install oracledb pydantic sqlalchemy
```

## Usage

### Initialization
```python
from your_module_name import OracleHelper
from pydantic import BaseModel

# Define your Pydantic model
class User(BaseModel):
    __TABLE_NAME__ = "users"
    id: int
    name: str
    email: str

# Initialize the OracleHelper
db_helper = OracleHelper(
    host="localhost",
    port=1521,
    sid="XE",
    user="system",
    password="oracle"
)
```

### Inserting Records
```python
# Insert a single record
user = User(id=1, name="John Doe", email="john@example.com")
db_helper.insert(user)

# Insert multiple records
users = [
    User(id=2, name="Jane Doe", email="jane@example.com"),
    User(id=3, name="Alice Smith", email="alice@example.com")
]
db_helper.insert_all(users)
```

### Upserting Records
```python
# Upsert a single record using the "id" field as the unique identifier
db_helper.upsert(user, using=["id"])

# Upsert multiple records
db_helper.upsert_all(users, using=["id"])
```

### Deleting Records
```python
# Delete a single record
db_helper.delete(user, using=["id"])

# Delete multiple records
db_helper.delete_all(users, using=["id"])
```

### Updating Records
```python
# Update a single record
user.name = "John Updated"
db_helper.update(user, using=["id"])

# Update multiple records
db_helper.update_all(users, using=["id"])
```

### Selecting Records
```python
# Select a single record with an optional WHERE clause
result = db_helper.select_one(User, where="id = 1")

# Select multiple records with an optional WHERE clause
results = db_helper.select_all(User, where="name LIKE 'Jane%'")
```

### Select in Batches
```python
# Select records in chunks of 100
for batch in db_helper.select_in_batches(User, where="id > 0", chunksize=100):
    for user in batch:
        print(user.name)
```

