import abc
from typing import Type, List

from pydantic import BaseModel


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


class OracleQueryHelper(DatabaseQueryHelperABC):
    @classmethod
    def generate_insert_query(cls, model: Type[BaseModel]) -> str:
        # Extract table name from the model
        table_name = model.__TABLE_NAME__

        # Prepare column names and placeholders for the query
        columns = []
        placeholders = []

        # Iterate over the model's fields
        for field in model.__annotations__.keys():
            if not field.startswith('__'):
                columns.append(field)
                placeholders.append(f":{field}")

        # Create the SQL query string
        columns_str = ", ".join(columns)
        placeholders_str = ", ".join(placeholders)
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders_str})"

        return query

    @classmethod
    def generate_upsert_query(cls, model: Type[BaseModel], using: List[str]) -> str:
        try:
            table_name = model.__TABLE_NAME__
        except AttributeError:
            raise AttributeError("Le modèle doit avoir un attribut __TABLE_NAME__")

        if len(using) == 0:
            raise ValueError("Aucun champ spécifié dans 'using'")

        columns = model.model_fields.keys()

        # Vérifiez que tous les champs 'using' existent dans le modèle
        for field in using:
            if field not in columns:
                raise ValueError(f"Le champ '{field}' n'existe pas dans le modèle {model.__name__}")

        # Construire la clause ON avec tous les champs dans 'using'
        key_conditions = " AND ".join(f"{table_name}.{field} = src.{field}" for field in using)

        # Créez une chaîne pour la clause WHEN MATCHED
        set_clause = ", ".join(f"{column} = :{column}" for column in columns if column not in using)

        # Créez une chaîne pour la clause WHEN NOT MATCHED
        insert_clause = ", ".join(columns)
        values_clause = ", ".join(f":{column}" for column in columns)

        # Construire la requête MERGE
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
            raise AttributeError("Le modèle doit avoir un attribut __TABLE_NAME__")

        if len(using) == 0:
            raise ValueError("Aucun champ spécifié dans 'using'")

        where_conditions = " AND ".join([f"{table_name}.{field} = :{field}" for field in using])

        query = f"DELETE FROM {table_name} WHERE {where_conditions}"

        return query.strip()

    @classmethod
    def generate_update_query(cls, model: Type[BaseModel], using: List[str]) -> str:
        try:
            table_name = model.__TABLE_NAME__
        except AttributeError:
            raise AttributeError("Le modèle doit avoir un attribut __TABLE_NAME__")

        if len(using) == 0:
            raise ValueError("Aucun champ spécifié dans 'using'")

        # Récupérer les champs du modèle pour la mise à jour
        fields = model.model_fields.keys()

        # Exclure les champs utilisés dans la condition `WHERE`
        fields_to_update = [field for field in fields if field not in using]

        if not fields_to_update:
            raise ValueError("Aucun champ à mettre à jour après exclusion des champs 'using'")

        # Construire les parties `SET` et `WHERE` de la requête
        set_clause = ", ".join([f"{table_name}.{field} = :{field}" for field in fields_to_update])
        where_conditions = " AND ".join([f"{table_name}.{field} = :{field}" for field in using])

        # Construire la requête `UPDATE`
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_conditions}"

        return query.strip()
