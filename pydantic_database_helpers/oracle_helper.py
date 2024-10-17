import abc
from typing import List, Optional, Type, TypeVar

from oracledb import makedsn, connect
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from pydantic_database_helpers.query_helper import OracleQueryHelper

T = TypeVar('T', bound=BaseModel)


class DatabaseHelper(abc.ABC):
    @abc.abstractmethod
    def insert(self, record: BaseModel) -> None:
        pass

    @abc.abstractmethod
    def insert_all(self, records: List[BaseModel]) -> None:
        pass

    @abc.abstractmethod
    def upsert(self, record: BaseModel, using: List[str]) -> None:
        pass

    @abc.abstractmethod
    def upsert_all(self, records: List[BaseModel], using: List[str]) -> None:
        pass

    @abc.abstractmethod
    def delete(self, record: BaseModel, using: List[str]) -> None:
        pass

    @abc.abstractmethod
    def delete_all(self, records: List[BaseModel], using: List[str]) -> None:
        pass

    @abc.abstractmethod
    def update(self, record: BaseModel, using: List[str]) -> None:
        pass

    @abc.abstractmethod
    def update_all(self, records: List[BaseModel], using: List[str]) -> None:
        pass

    @abc.abstractmethod
    def select_one(self, model: Type[T], where: Optional[str]) -> T:
        pass

    @abc.abstractmethod
    def select_all(self, model: Type[T], where: Optional[str]) -> List[T]:
        pass

    @abc.abstractmethod
    def clean_up(self) -> None:
        pass


class OracleHelper(DatabaseHelper, OracleQueryHelper):

    def __init__(self):
        dsn = makedsn("localhost", 1521, sid="XE")
        self.connection = connect(user="system", password="oracle", dsn=dsn)
        self.engine = create_engine(f"oracle+oracledb://", creator=lambda: self.connection)

    def insert(self, record: BaseModel) -> None:
        query = self.generate_insert_query(type(record))
        values = record.model_dump(exclude_unset=True)
        self._execute(query, values)

    def insert_all(self, records: List[BaseModel]) -> None:
        if not records:
            print("Nothing to insert")
            return None
        query = self.generate_insert_query(type(records[0]))
        values = [r.model_dump(exclude_unset=True) for r in records]
        self._executemany(query, values)

    def upsert(self, record: BaseModel, using: List[str]) -> None:
        query = self.generate_upsert_query(type(record), using)
        values = record.model_dump(exclude_unset=True)
        self._execute(query, values)

    def upsert_all(self, records: List[BaseModel], using: List[str]) -> None:
        if not records:
            print("Nothing to upsert")
            return None
        query = self.generate_upsert_query(type(records[0]), using)
        values = [r.model_dump(exclude_unset=True) for r in records]
        self._executemany(query, values)

    def delete(self, record: BaseModel, using: List[str]) -> None:
        query = self.generate_delete_query(type(record), using)
        values = record.model_dump(exclude_unset=True)
        self._execute(query, values)

    def delete_all(self, records: List[BaseModel], using: List[str]) -> None:
        if not records:
            print("Nothing to delete")
            return None
        query = self.generate_delete_query(type(records[0]), using)
        values = [r.model_dump(exclude_unset=True, include=set(using)) for r in records]
        self._executemany(query, values)

    def update(self, record: BaseModel, using: List[str]) -> None:
        query = self.generate_update_query(type(record), using)
        values = record.model_dump(exclude_unset=True)
        self._execute(query, values)

    def update_all(self, records: List[BaseModel], using: List[str]) -> None:
        if not records:
            print("Nothing to update")
            return None
        query = self.generate_update_query(type(records[0]), using)
        values = [r.model_dump(exclude_unset=True) for r in records]
        self._executemany(query, values)

    def select_one(self, model: Type[T], where: Optional[str] = None) -> Optional[T]:
        query = self.generate_select_query(model, where)
        with Session(self.engine) as session:
            try:
                result = session.execute(text(query)).fetchone()
                if result:
                    column_names = [field_name for field_name in model.model_fields.keys()]
                    result_dict = dict(zip(column_names, result))
                    return model(**result_dict)
                return None
            except Exception as e:
                print(f"Erreur lors de l'execution de la requête dans la base de données : {e}")
                session.rollback()
                raise e

    def select_all(self, model: Type[T], where: Optional[str] = None) -> List[T]:
        query = self.generate_select_query(model, where)
        with Session(self.engine) as session:
            try:
                result = session.execute(text(query)).fetchall()
                if result:
                    column_names = [field_name for field_name in model.model_fields.keys()]
                    return [model(**dict(zip(column_names, r))) for r in result]
                return []
            except Exception as e:
                print(f"Erreur lors de l'execution de la requête dans la base de données : {e}")
                session.rollback()
                raise e

    def _execute(self, query: str, values):
        with Session(self.engine) as session:
            try:
                session.execute(text(query), values)
                session.commit()
            except Exception as e:
                print(f"Erreur lors de l'execution de la requête dans la base de données : {e}")
                session.rollback()
                raise e

    def _executemany(self, query: str, values):
        with self.connection.cursor() as cursor:
            try:
                cursor.executemany(query, values)
                self.connection.commit()
            except Exception as e:
                print(f"Erreur lors de l'execution de la requête dans la base de données : {e}")
                self.connection.rollback()
                raise e

    def clean_up(self) -> None:
        """Ferme la connexion et toutes les sessions ouvertes."""
        try:
            # Fermer l'engine
            if self.engine:
                self.engine.dispose()
                print("Engine fermé.")

            # Fermer la connexion à la base de données
            if self.connection:
                self.connection.close()
                print("Connexion à la base de données fermée.")
        except Exception as e:
            print(f"Erreur lors de la fermeture des ressources : {e}")
