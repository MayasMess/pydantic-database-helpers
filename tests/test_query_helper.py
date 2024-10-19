from datetime import datetime, date
from decimal import Decimal
from typing import Optional, ClassVar

import pytest
from pydantic import BaseModel

from pydantic_database_helpers.query_helper import OracleQueryHelper, MISSING_TABLE_NAME_ATTR_MSG, EMPTY_USING_MSG
from tests.models import SimpleTable, NoTableNameModel, DummyModel, ExampleModel


# Classe de test pour hériter de DatabaseQueryHelperABC pour les tests
class TestOracleQueryHelper(OracleQueryHelper):
    pass


# Fixtures pour les tests
@pytest.fixture
def helper():
    return TestOracleQueryHelper()


@pytest.fixture
def simple_model():
    return SimpleTable(
        id=1,
        name="Test Name",
        created_at=datetime.now(),
        updated_at=datetime.now(),
        is_active=True,
        salary=1000.00,
        birth_date=date(1990, 1, 1),
        decimal_value=Decimal("1234.56")
    )


# Test 1: Test de la génération de la requête pour un modèle valide
def test_generate_insert_query_valid(helper, simple_model):
    query = helper.generate_insert_query(type(simple_model))
    expected_query = (
        "INSERT INTO simple_table (id, name, created_at, updated_at, is_active, salary, birth_date, decimal_value) "
        "VALUES (:id, :name, :created_at, :updated_at, :is_active, :salary, :birth_date, :decimal_value)"
    )
    assert query == expected_query


# Test 2: Test de la génération de la requête pour un modèle avec des valeurs optionnelles manquantes
def test_generate_insert_query_with_optional_fields(helper):
    class PartialModel(BaseModel):
        __TABLE_NAME__: ClassVar[str] = "partial_table"
        id: int
        name: Optional[str] = None

    query = helper.generate_insert_query(PartialModel)
    expected_query = "INSERT INTO partial_table (id, name) VALUES (:id, :name)"
    assert query == expected_query


# Test 3: Test pour vérifier qu'une exception est levée si le modèle n'a pas de __TABLE_NAME__
def test_generate_insert_query_without_table_name(helper):
    class NoTableNameModel(BaseModel):
        id: int

    with pytest.raises(AttributeError):
        helper.generate_insert_query(NoTableNameModel)


# Test 4: Test de la requête avec un modèle contenant un champ supplémentaire
def test_generate_insert_query_with_extra_field(helper):
    class ExtraFieldModel(BaseModel):
        __TABLE_NAME__: ClassVar[str] = "extra_table"
        id: int
        name: str
        extra_field: Optional[int]

    query = helper.generate_insert_query(ExtraFieldModel)
    expected_query = (
        "INSERT INTO extra_table (id, name, extra_field) VALUES (:id, :name, :extra_field)"
    )
    assert query == expected_query


# Test 5: Test de la requête avec un modèle vide
def test_generate_insert_query_empty_model(helper):
    class EmptyModel(BaseModel):
        __TABLE_NAME__: ClassVar[str] = "empty_table"

    query = helper.generate_insert_query(EmptyModel)
    expected_query = "INSERT INTO empty_table () VALUES ()"
    assert query == expected_query


def test_generate_upsert_query_valid():
    using_fields = ["id", "name", "created_at", "updated_at", "is_active", "salary"]

    expected_query = (
        "MERGE INTO simple_table USING ("
        "SELECT :id as id, :name as name, :created_at as created_at, :updated_at as updated_at, :is_active as is_active,"
        " :salary as salary FROM dual) src "
        "ON ("
        "simple_table.id = src.id AND simple_table.name = src.name AND simple_table.created_at = src.created_at AND "
        "simple_table.updated_at = src.updated_at AND simple_table.is_active = src.is_active "
        "AND simple_table.salary = src.salary) "
        "WHEN MATCHED THEN "
        "UPDATE SET birth_date = :birth_date, decimal_value = :decimal_value "
        "WHEN NOT MATCHED THEN "
        "INSERT (id, name, created_at, updated_at, is_active, salary, birth_date, decimal_value) "
        "VALUES (:id, :name, :created_at, :updated_at, :is_active, :salary, :birth_date, :decimal_value)"
    )

    query = OracleQueryHelper.generate_upsert_query(SimpleTable, using=using_fields)
    assert query.replace(' ', '').replace('\n', '') == expected_query.replace(' ', '').replace('\n', '')


def test_generate_upsert_query_invalid_field():
    using_fields = ["id", "non_existing_field"]

    with pytest.raises(ValueError, match="The field 'non_existing_field' does not exist in the model SimpleTable"):
        OracleQueryHelper.generate_upsert_query(SimpleTable, using=using_fields)


def test_generate_upsert_query_missing_table_name():
    class InvalidTableModel(BaseModel):
        id: int
        name: str

    using_fields = ["id", "name"]

    with pytest.raises(AttributeError, match=MISSING_TABLE_NAME_ATTR_MSG):
        OracleQueryHelper.generate_upsert_query(InvalidTableModel, using=using_fields)


def test_generate_upsert_query_empty_using_list():
    using_fields = []

    with pytest.raises(ValueError, match=EMPTY_USING_MSG):
        OracleQueryHelper.generate_upsert_query(SimpleTable, using=using_fields)


def test_generate_upsert_query_single_field():
    using_fields = ["id"]

    expected_query = """
    MERGE INTO simple_table USING (SELECT :id as id FROM dual) src
    ON (simple_table.id = src.id)
    WHEN MATCHED THEN
        UPDATE SET name = :name, created_at = :created_at, updated_at = :updated_at, is_active = :is_active, 
        salary = :salary, birth_date = :birth_date, decimal_value = :decimal_value
    WHEN NOT MATCHED THEN
        INSERT (id, name, created_at, updated_at, is_active, salary, birth_date, decimal_value)
        VALUES (:id, :name, :created_at, :updated_at, :is_active, :salary, :birth_date, :decimal_value)
    """.strip()

    query = OracleQueryHelper.generate_upsert_query(SimpleTable, using=using_fields)
    assert query.replace(' ', '').replace('\n', '') == expected_query.replace(' ', '').replace('\n', '')


def test_generate_upsert_query_multiple_fields():
    using_fields = ["id", "name"]

    expected_query = """
    MERGE INTO simple_table USING (SELECT :id as id, :name as name FROM dual) src
    ON (simple_table.id = src.id AND simple_table.name = src.name)
    WHEN MATCHED THEN
        UPDATE SET created_at = :created_at, updated_at = :updated_at, is_active = :is_active, 
        salary = :salary, birth_date = :birth_date, decimal_value = :decimal_value
    WHEN NOT MATCHED THEN
        INSERT (id, name, created_at, updated_at, is_active, salary, birth_date, decimal_value)
        VALUES (:id, :name, :created_at, :updated_at, :is_active, :salary, :birth_date, :decimal_value)
    """.strip()

    query = OracleQueryHelper.generate_upsert_query(SimpleTable, using=using_fields)
    assert query.replace(' ', '').replace('\n', '') == expected_query.replace(' ', '').replace('\n', '')


def test_generate_delete_query_valid():
    using_fields = ["id", "name"]
    expected_query = "DELETE FROM simple_table WHERE simple_table.id = :id AND simple_table.name = :name"

    query = OracleQueryHelper.generate_delete_query(SimpleTable, using=using_fields)
    assert query == expected_query


def test_generate_delete_query_no_using_fields():
    using_fields = []

    with pytest.raises(ValueError, match=EMPTY_USING_MSG):
        OracleQueryHelper.generate_delete_query(SimpleTable, using=using_fields)


def test_generate_delete_query_missing_table_name():
    class InvalidTableModel(BaseModel):
        pass  # Pas de __TABLE_NAME__

    using_fields = ["id"]

    with pytest.raises(AttributeError, match=MISSING_TABLE_NAME_ATTR_MSG):
        OracleQueryHelper.generate_delete_query(InvalidTableModel, using=using_fields)


def test_generate_delete_query_single_using_field():
    using_fields = ["id"]
    expected_query = "DELETE FROM simple_table WHERE simple_table.id = :id"

    query = OracleQueryHelper.generate_delete_query(SimpleTable, using=using_fields)
    assert query == expected_query


def test_generate_delete_query_multiple_using_fields():
    using_fields = ["id", "name", "age"]
    expected_query = "DELETE FROM simple_table WHERE simple_table.id = :id AND simple_table.name = :name AND simple_table.age = :age"

    query = OracleQueryHelper.generate_delete_query(SimpleTable, using=using_fields)
    assert query == expected_query


def test_generate_update_query_valid():
    using_fields = ["id"]
    expected_query = (
        "UPDATE simple_table SET simple_table.name = :name, simple_table.created_at = :created_at, "
        "simple_table.updated_at = :updated_at, simple_table.is_active = :is_active, "
        "simple_table.salary = :salary, simple_table.birth_date = :birth_date, simple_table.decimal_value = :decimal_value "
        "WHERE simple_table.id = :id"
    )

    query = OracleQueryHelper.generate_update_query(SimpleTable, using=using_fields)

    assert query == expected_query


def test_generate_update_query_multiple_using_fields():
    using_fields = ["id", "created_at"]
    expected_query = (
        "UPDATE simple_table SET simple_table.name = :name, simple_table.updated_at = :updated_at, "
        "simple_table.is_active = :is_active, simple_table.salary = :salary, simple_table.birth_date = :birth_date, "
        "simple_table.decimal_value = :decimal_value WHERE simple_table.id = :id AND simple_table.created_at = :created_at"
    )

    query = OracleQueryHelper.generate_update_query(SimpleTable, using=using_fields)

    assert query == expected_query


def test_generate_update_query_no_table_name():
    using_fields = ["id"]

    with pytest.raises(AttributeError, match=MISSING_TABLE_NAME_ATTR_MSG):
        OracleQueryHelper.generate_update_query(NoTableNameModel, using=using_fields)


def test_generate_update_query_no_using_fields():
    using_fields = []

    with pytest.raises(ValueError, match=EMPTY_USING_MSG):
        OracleQueryHelper.generate_update_query(SimpleTable, using=using_fields)


def test_generate_update_query_all_fields_in_using():
    using_fields = ["id", "name", "created_at", "updated_at", "is_active", "salary", "birth_date", "decimal_value"]

    with pytest.raises(ValueError, match="No fields to update after excluding 'using' fields."):
        OracleQueryHelper.generate_update_query(SimpleTable, using=using_fields)


def test_generate_select_query_no_where():
    # Test pour une requête sans clause WHERE
    expected_query = "SELECT id, name, age FROM test_table"
    query = OracleQueryHelper.generate_select_query(DummyModel)
    assert query == expected_query


def test_generate_select_query_with_where():
    # Test pour une requête avec une clause WHERE
    expected_query = "SELECT id, name, age FROM test_table WHERE age > 30"
    query = OracleQueryHelper.generate_select_query(DummyModel, where="age > 30")
    assert query == expected_query


def test_generate_select_query_with_multiple_where():
    # Test pour une requête avec une clause WHERE
    expected_query = "SELECT id, name, age FROM test_table WHERE age > 30 and name = 'hello'"
    query = OracleQueryHelper.generate_select_query(DummyModel, where="age > 30 and name = 'hello'")
    assert query == expected_query


def test_generate_select_query_no_table_name():
    # Test pour vérifier le comportement quand le modèle n'a pas de __TABLE_NAME__
    class InvalidModel(BaseModel):
        id: int
        name: Optional[str]

    with pytest.raises(AttributeError, match=MISSING_TABLE_NAME_ATTR_MSG):
        OracleQueryHelper.generate_select_query(InvalidModel)


def test_generate_select_query_no_fields():
    # Test pour un modèle sans champs (rare mais possible)
    class EmptyModel(BaseModel):
        __TABLE_NAME__: str = "empty_table"

    expected_query = "SELECT  FROM empty_table"
    query = OracleQueryHelper.generate_select_query(EmptyModel)
    assert query == expected_query


def test_generate_select_query_valid_where():
    where_clause = "id = 1 AND name = 'John'"
    query = OracleQueryHelper.generate_select_query(ExampleModel, where=where_clause)
    assert query == "SELECT id, name FROM example_table WHERE id = 1 AND name = 'John'"


def test_generate_select_query_invalid_where():
    # Liste de clauses WHERE dangereuses à tester
    dangerous_clauses = [
        "1=1; DROP TABLE users",  # Tentative de suppression de table
        "name = 'John' --",  # Tentative de commentaire SQL
        "name = 'John'/* Comment */",  # Tentative de commentaire en bloc
        "id = 1; SELECT * FROM sensitive",  # Tentative de sous-requête
        "name = 'John'; EXEC xp_cmdshell('dir')",  # Injection de commande
    ]

    for where in dangerous_clauses:
        with pytest.raises(ValueError) as exc_info:
            OracleQueryHelper.generate_select_query(ExampleModel, where=where)
        assert "Clause WHERE invalid" in str(exc_info.value)


def test_generate_select_query_empty_where():
    # Test avec une clause WHERE vide
    query = OracleQueryHelper.generate_select_query(ExampleModel, where="")
    assert query == "SELECT id, name FROM example_table"
