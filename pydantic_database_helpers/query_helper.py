import abc
from typing import Type, List, Optional

from pydantic import BaseModel


MISSING_TABLE_NAME_ATTR_MSG = "The model must have a __TABLE_NAME__ attribute."
EMPTY_USING_MSG = "No fields specified in 'using'."


class DatabaseQueryHelperABC(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def generate_insert_query(cls, model: Type[BaseModel]) -> str:
        pass

    @classmethod
    @abc.abstractmethod
    def generate_upsert_query(cls, model: Type[BaseModel], using: List[str]) -> str:
        pass

    @classmethod
    @abc.abstractmethod
    def generate_delete_query(cls, model: Type[BaseModel], using: List[str]) -> str:
        pass

    @classmethod
    @abc.abstractmethod
    def generate_update_query(cls, model: Type[BaseModel], using: List[str]) -> str:
        pass

    @classmethod
    @abc.abstractmethod
    def generate_select_query(cls, model: Type[BaseModel], where: Optional[str] = None) -> str:
        pass


class OracleQueryHelper(DatabaseQueryHelperABC):
    @classmethod
    def generate_insert_query(cls, model: Type[BaseModel]) -> str:
        table_name = model.__TABLE_NAME__

        columns = []
        placeholders = []

        for field in model.__annotations__.keys():
            if not field.startswith('__'):
                columns.append(field)
                placeholders.append(f":{field}")

        columns_str = ", ".join(columns)
        placeholders_str = ", ".join(placeholders)
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders_str})"

        return query

    @classmethod
    def generate_upsert_query(cls, model: Type[BaseModel], using: List[str]) -> str:
        try:
            table_name = model.__TABLE_NAME__
        except AttributeError:
            raise AttributeError(MISSING_TABLE_NAME_ATTR_MSG)

        if len(using) == 0:
            raise ValueError(EMPTY_USING_MSG)

        columns = model.model_fields.keys()

        for field in using:
            if field not in columns:
                raise ValueError(f"The field '{field}' does not exist in the model {model.__name__}")

        key_conditions = " AND ".join(f"{table_name}.{field} = src.{field}" for field in using)
        set_clause = ", ".join(f"{column} = :{column}" for column in columns if column not in using)
        insert_clause = ", ".join(columns)
        values_clause = ", ".join(f":{column}" for column in columns)

        query = f"""
            MERGE INTO {table_name} USING (SELECT {', '.join(f':{field} as {field}' for field in using)} FROM dual) src
            ON ({key_conditions})
            WHEN MATCHED THEN
                UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_clause})
                VALUES ({values_clause})
            """

        return query

    @classmethod
    def generate_delete_query(cls, model: Type[BaseModel], using: List[str]) -> str:
        try:
            table_name = model.__TABLE_NAME__
        except AttributeError:
            raise AttributeError(MISSING_TABLE_NAME_ATTR_MSG)

        if len(using) == 0:
            raise ValueError(EMPTY_USING_MSG)

        where_conditions = " AND ".join([f"{table_name}.{field} = :{field}" for field in using])

        query = f"DELETE FROM {table_name} WHERE {where_conditions}"

        return query.strip()

    @classmethod
    def generate_update_query(cls, model: Type[BaseModel], using: List[str]) -> str:
        try:
            table_name = model.__TABLE_NAME__
        except AttributeError:
            raise AttributeError(MISSING_TABLE_NAME_ATTR_MSG)

        if len(using) == 0:
            raise ValueError(EMPTY_USING_MSG)

        fields = model.model_fields.keys()

        fields_to_update = [field for field in fields if field not in using]

        if not fields_to_update:
            raise ValueError("No fields to update after excluding 'using' fields.")

        set_clause = ", ".join([f"{table_name}.{field} = :{field}" for field in fields_to_update])
        where_conditions = " AND ".join([f"{table_name}.{field} = :{field}" for field in using])

        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_conditions}"

        return query.strip()

    @classmethod
    def generate_select_query(cls, model: Type[BaseModel], where: Optional[str] = None) -> str:
        try:
            table_name = model.__TABLE_NAME__
        except AttributeError:
            raise AttributeError(MISSING_TABLE_NAME_ATTR_MSG)

        fields = model.model_fields.keys()
        columns = ", ".join(fields)

        if where:
            dangerous_patterns = [";", "--", "/*", "*/", "xp_", "exec", "drop", "select"]
            if any(pattern.lower() in where.lower() for pattern in dangerous_patterns):
                raise ValueError(f"Clause WHERE invalid : {where}")

        query = f"SELECT {columns} FROM {table_name}"

        if where:
            query += f" WHERE {where}"

        return query.strip()
