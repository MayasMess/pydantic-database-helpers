import abc
import logging
from typing import List, Optional, Type, TypeVar, Generator

from oracledb import makedsn, connect
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from pydantic_database_helpers.query_helper import OracleQueryHelper

logger = logging.getLogger(__name__)
T = TypeVar('T', bound=BaseModel)
DATABASE_ACTION_ERROR_MSG = "Error while executing the query in the database."


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

    def __init__(self, host="localhost", port=1521, sid="XE", service_name=None, user="system", password="oracle"):
        self.dsn = makedsn(host, port, sid=sid, service_name=service_name)
        self.user = user
        self.password = password
        self._connect()

    def _connect(self):
        """Establishes a new connection and engine."""
        self.connection = connect(user=self.user, password=self.password, dsn=self.dsn)
        self.engine = create_engine("oracle+oracledb://", creator=lambda: self.connection)

    def insert(self, record: BaseModel) -> None:
        query = self.generate_insert_query(type(record))
        values = record.model_dump(exclude_unset=True)
        self._execute(query, values)

    def insert_all(self, records: List[BaseModel]) -> None:
        if not records:
            logger.warning("Nothing to insert")
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
            logger.warning("Nothing to upsert")
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
            logger.warning("Nothing to delete")
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
            logger.warning("Nothing to update")
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
                logger.error(f"{DATABASE_ACTION_ERROR_MSG} : {e}")
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
                logger.error(f"{DATABASE_ACTION_ERROR_MSG} : {e}")
                session.rollback()
                raise e

    def select_in_batches(
            self,
            model: Type[T],
            where: Optional[str] = None,
            chunksize: int = 100
    ) -> Generator[List[T], None, None]:
        query = self.generate_select_query(model, where)
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query)
                while True:
                    result_chunk = cursor.fetchmany(chunksize)
                    if not result_chunk:
                        break

                    column_names = [field_name for field_name in model.model_fields.keys()]
                    chunk = [model(**dict(zip(column_names, row))) for row in result_chunk]

                    yield chunk
            except Exception as e:
                logger.error(f"{DATABASE_ACTION_ERROR_MSG} : {e}")
                self.connection.rollback()
                raise e

    def _execute(self, query: str, values):
        with Session(self.engine) as session:
            try:
                session.execute(text(query), values)
                session.commit()
            except Exception as e:
                logger.error(f"{DATABASE_ACTION_ERROR_MSG} : {e}")
                session.rollback()
                raise e

    def _executemany(self, query: str, values):
        with self.connection.cursor() as cursor:
            try:
                cursor.executemany(query, values)
                self.connection.commit()
            except Exception as e:
                logger.error(f"{DATABASE_ACTION_ERROR_MSG} : {e}")
                self.connection.rollback()
                raise e

    def clean_up(self) -> None:
        try:
            # Fermer l'engine
            if self.engine:
                self.engine.dispose()
                logger.info("Engine closed.")

            # Fermer la connexion à la base de données
            if self.connection and self.connection.is_healthy():
                self.connection.close()
                logger.info("Database connection closed.")
        except Exception as e:
            logger.error(f"Error on resources cleanup : {e}")
