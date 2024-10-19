import logging
import warnings
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from pydantic_database_helpers.database_helper import OracleHelper
from tests.models import SimpleTable

logger = logging.getLogger(__name__)


def try_oracle_connection():
    try:
        return OracleHelper()
    except Exception as e:
        warnings.warn("Could not connect to Oracle. Please configure connection to be able to run integration tests")
    return None


def test_insert_integration():
    oracle_helper = try_oracle_connection()
    if oracle_helper is None:
        return

    record = SimpleTable(
        id=1,
        name="Test Name",
        created_at="2024-10-17 12:00:00",
        updated_at="2024-10-17 12:00:00",
        is_active=True,
        salary=1000.00,
        birth_date="1990-01-01",
        decimal_value=1234.56
    )

    oracle_helper.insert(record)

    with Session(oracle_helper.engine) as session:
        result = session.execute(text("SELECT * FROM simple_table WHERE id = :id"), {'id': record.id})
        row = result.fetchone()

        assert row is not None
        assert row[0] == record.id
        assert row[1] == record.name
        assert row[2] == datetime(2024, 10, 17, 12, 0, 0)
        assert row[3] == datetime(2024, 10, 17, 12, 0, 0)
        assert row[4] == int(record.is_active)  # Convertir en entier pour le test
        assert row[5] == record.salary
        assert row[6] == datetime(1990, 1, 1, 0, 0)
        assert row[7] == record.decimal_value

        session.execute(text("DELETE FROM simple_table WHERE id = :id"), {'id': record.id})
        session.commit()
    oracle_helper.clean_up()


def test_insert_all_integration():
    oracle_helper = try_oracle_connection()
    if oracle_helper is None:
        return

    records = [
        SimpleTable(id=2, name="Test Name 2", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=True, salary=2000.00, birth_date="1991-01-01", decimal_value=2345.67),
        SimpleTable(id=3, name="Test Name 3", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=False, salary=3000.00, birth_date="1992-01-01", decimal_value=3456.78)
    ]

    oracle_helper.insert_all(records)

    with Session(oracle_helper.engine) as session:
        for record in records:
            result = session.execute(text("SELECT * FROM simple_table WHERE id = :id"), {'id': record.id})
            row = result.fetchone()

            assert row is not None
            assert row[0] == record.id
            assert row[1] == record.name
            assert row[2] == datetime(2024, 10, 17, 12, 0, 0)
            assert row[3] == datetime(2024, 10, 17, 12, 0, 0)
            assert row[4] == int(record.is_active)  # Convertir en entier pour le test
            assert row[5] == record.salary
            assert row[6] == datetime(1991, 1, 1) if record.id == 2 else datetime(1992, 1, 1)
            assert row[7] == record.decimal_value

            session.execute(text("DELETE FROM simple_table WHERE id = :id"), {'id': record.id})
            session.commit()

    oracle_helper.clean_up()


def test_upsert_integration():
    oracle_helper = try_oracle_connection()
    if oracle_helper is None:
        return

    record = SimpleTable(
        id=4,
        name="John Doe",
        created_at=datetime.now(),
        updated_at=datetime.now(),
        is_active=True,
        salary=50000.0,
        birth_date=datetime(1990, 1, 1, 0, 0).date(),
        decimal_value=123.45
    )

    oracle_helper.upsert(record, using=["id"])

    with Session(oracle_helper.engine) as session:
        result = session.execute(
            text("SELECT * FROM simple_table WHERE id = :id"), {"id": record.id}
        ).fetchone()

        assert result is not None
        assert result[1] == record.name
        assert result[2] == record.created_at.replace(microsecond=0)
        assert result[3] == record.updated_at.replace(microsecond=0)
        assert result[4] == record.is_active
        assert result[5] == record.salary
        assert result[6].date() == record.birth_date
        assert result[7] == record.decimal_value

    record.salary = 100000.0
    record.is_active = False
    oracle_helper.upsert(record, using=["id"])

    with Session(oracle_helper.engine) as session:
        result = session.execute(
            text("SELECT * FROM simple_table WHERE id = :id"), {"id": record.id}
        ).fetchone()

        assert result is not None
        assert result[1] == record.name
        assert result[2] == record.created_at.replace(microsecond=0)
        assert result[3] == record.updated_at.replace(microsecond=0)
        assert result[4] == record.is_active
        assert result[5] == record.salary
        assert result[6].date() == record.birth_date
        assert result[7] == record.decimal_value

        session.execute(text("DELETE FROM simple_table WHERE id = :id"), {'id': record.id})
        session.commit()

    oracle_helper.clean_up()


def test_upsert_all_integration():
    oracle_helper = try_oracle_connection()
    if oracle_helper is None:
        return

    records = [
        SimpleTable(id=5, name="Test Name 2", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=True, salary=2000.00, birth_date="1991-01-01", decimal_value=2345.67),
        SimpleTable(id=6, name="Test Name 3", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=False, salary=3000.00, birth_date="1992-01-01", decimal_value=3456.78)
    ]

    oracle_helper.upsert_all(records, using=["id"])

    with Session(oracle_helper.engine) as session:
        for record in records:
            result = session.execute(
                text("SELECT * FROM simple_table WHERE id = :id"), {"id": record.id}
            ).fetchone()

            assert result is not None
            assert result[1] == record.name
            assert result[2] == record.created_at.replace(microsecond=0)
            assert result[3] == record.updated_at.replace(microsecond=0)
            assert result[4] == record.is_active
            assert result[5] == record.salary
            assert result[6].date() == record.birth_date
            assert result[7] == record.decimal_value

    for record in records:
        record.salary = 100000.0
        record.is_active = False
        oracle_helper.upsert(record, using=["id"])

    with Session(oracle_helper.engine) as session:
        for record in records:
            result = session.execute(
                text("SELECT * FROM simple_table WHERE id = :id"), {"id": record.id}
            ).fetchone()

            assert result is not None
            assert result[1] == record.name
            assert result[2] == record.created_at.replace(microsecond=0)
            assert result[3] == record.updated_at.replace(microsecond=0)
            assert result[4] == record.is_active
            assert result[5] == record.salary
            assert result[6].date() == record.birth_date
            assert result[7] == record.decimal_value

            session.execute(text("DELETE FROM simple_table WHERE id = :id"), {'id': record.id})
            session.commit()

    oracle_helper.clean_up()


def test_delete_integration():
    oracle_helper = try_oracle_connection()
    if oracle_helper is None:
        return

    record = SimpleTable(
        id=7,
        name="John Doe",
        created_at=datetime.now(),
        updated_at=datetime.now(),
        is_active=True,
        salary=50000.0,
        birth_date=datetime(1990, 1, 1, 0, 0).date(),
        decimal_value=123.45
    )

    oracle_helper.insert(record)

    with Session(oracle_helper.engine) as session:
        result = session.execute(
            text("SELECT * FROM simple_table WHERE id = :id"), {"id": record.id}
        ).fetchone()

        assert result is not None
        assert result[1] == record.name
        assert result[2] == record.created_at.replace(microsecond=0)
        assert result[3] == record.updated_at.replace(microsecond=0)
        assert result[4] == record.is_active
        assert result[5] == record.salary
        assert result[6].date() == record.birth_date
        assert result[7] == record.decimal_value

    oracle_helper.delete(record, using=["id"])

    with Session(oracle_helper.engine) as session:
        result = session.execute(
            text("SELECT * FROM simple_table WHERE id = :id"), {"id": record.id}
        ).fetchone()

        assert result is None

        session.execute(text("DELETE FROM simple_table WHERE id = :id"), {'id': record.id})
        session.commit()

    oracle_helper.clean_up()


def test_delete_all_integration():
    oracle_helper = try_oracle_connection()
    if oracle_helper is None:
        return

    records = [
        SimpleTable(id=8, name="Test Name 2", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=True, salary=2000.00, birth_date="1991-01-01", decimal_value=2345.67),
        SimpleTable(id=9, name="Test Name 3", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=False, salary=3000.00, birth_date="1992-01-01", decimal_value=3456.78)
    ]

    oracle_helper.insert_all(records)

    with Session(oracle_helper.engine) as session:
        for record in records:
            result = session.execute(
                text("SELECT * FROM simple_table WHERE id = :id"), {"id": record.id}
            ).fetchone()

            assert result is not None
            assert result[1] == record.name
            assert result[2] == record.created_at.replace(microsecond=0)
            assert result[3] == record.updated_at.replace(microsecond=0)
            assert result[4] == record.is_active
            assert result[5] == record.salary
            assert result[6].date() == record.birth_date
            assert result[7] == record.decimal_value

    oracle_helper.delete_all(records, using=["id"])

    with Session(oracle_helper.engine) as session:
        for record in records:
            result = session.execute(
                text("SELECT * FROM simple_table WHERE id = :id"), {"id": record.id}
            ).fetchone()

            assert result is None

            session.execute(text("DELETE FROM simple_table WHERE id = :id"), {'id': record.id})
            session.commit()

    oracle_helper.clean_up()


def test_update_integration():
    oracle_helper = try_oracle_connection()
    if oracle_helper is None:
        return

    record = SimpleTable(
        id=10,
        name="John Doe",
        created_at=datetime.now(),
        updated_at=datetime.now(),
        is_active=True,
        salary=50000.0,
        birth_date=datetime(1990, 1, 1, 0, 0).date(),
        decimal_value=123.45
    )

    oracle_helper.insert(record)

    with Session(oracle_helper.engine) as session:
        result = session.execute(
            text("SELECT * FROM simple_table WHERE id = :id"), {"id": record.id}
        ).fetchone()

        assert result is not None
        assert result[1] == record.name
        assert result[2] == record.created_at.replace(microsecond=0)
        assert result[3] == record.updated_at.replace(microsecond=0)
        assert result[4] == record.is_active
        assert result[5] == record.salary
        assert result[6].date() == record.birth_date
        assert result[7] == record.decimal_value

    record.salary = 100000.0
    record.is_active = False
    oracle_helper.update(record, using=["id"])

    with Session(oracle_helper.engine) as session:
        result = session.execute(
            text("SELECT * FROM simple_table WHERE id = :id"), {"id": record.id}
        ).fetchone()

        assert result is not None
        assert result[1] == record.name
        assert result[2] == record.created_at.replace(microsecond=0)
        assert result[3] == record.updated_at.replace(microsecond=0)
        assert result[4] == record.is_active
        assert result[5] == record.salary
        assert result[6].date() == record.birth_date
        assert result[7] == record.decimal_value

        session.execute(text("DELETE FROM simple_table WHERE id = :id"), {'id': record.id})
        session.commit()

    oracle_helper.clean_up()


def test_update_all_integration():
    oracle_helper = try_oracle_connection()
    if oracle_helper is None:
        return

    records = [
        SimpleTable(id=11, name="Test Name 2", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=True, salary=2000.00, birth_date="1991-01-01", decimal_value=2345.67),
        SimpleTable(id=12, name="Test Name 3", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=False, salary=3000.00, birth_date="1992-01-01", decimal_value=3456.78)
    ]

    oracle_helper.insert_all(records)

    with Session(oracle_helper.engine) as session:
        for record in records:
            result = session.execute(
                text("SELECT * FROM simple_table WHERE id = :id"), {"id": record.id}
            ).fetchone()

            assert result is not None
            assert result[1] == record.name
            assert result[2] == record.created_at.replace(microsecond=0)
            assert result[3] == record.updated_at.replace(microsecond=0)
            assert result[4] == record.is_active
            assert result[5] == record.salary
            assert result[6].date() == record.birth_date
            assert result[7] == record.decimal_value

    for record in records:
        record.salary = 100000.0
        record.is_active = False
        oracle_helper.update(record, using=["id"])

    with Session(oracle_helper.engine) as session:
        for record in records:
            result = session.execute(
                text("SELECT * FROM simple_table WHERE id = :id"), {"id": record.id}
            ).fetchone()

            assert result is not None
            assert result[1] == record.name
            assert result[2] == record.created_at.replace(microsecond=0)
            assert result[3] == record.updated_at.replace(microsecond=0)
            assert result[4] == record.is_active
            assert result[5] == record.salary
            assert result[6].date() == record.birth_date
            assert result[7] == record.decimal_value

            session.execute(text("DELETE FROM simple_table WHERE id = :id"), {'id': record.id})
            session.commit()

    oracle_helper.clean_up()


def test_select_integration():
    oracle_helper = try_oracle_connection()
    if oracle_helper is None:
        return

    record = SimpleTable(
        id=13,
        name="Test Name",
        created_at="2024-10-17 12:00:00",
        updated_at="2024-10-17 12:00:00",
        is_active=True,
        salary=1000.00,
        birth_date="1990-01-01",
        decimal_value=1234.56
    )

    oracle_helper.insert(record)

    with Session(oracle_helper.engine) as session:
        result = session.execute(text("SELECT * FROM simple_table WHERE id = :id"), {'id': record.id})
        row = result.fetchone()

        assert row is not None
        assert row[0] == record.id
        assert row[1] == record.name
        assert row[2] == datetime(2024, 10, 17, 12, 0, 0)
        assert row[3] == datetime(2024, 10, 17, 12, 0, 0)
        assert row[4] == int(record.is_active)  # Convertir en entier pour le test
        assert row[5] == record.salary
        assert row[6] == datetime(1990, 1, 1, 0, 0)
        assert row[7] == record.decimal_value

    res = oracle_helper.select_one(SimpleTable)
    assert res == record

    with Session(oracle_helper.engine) as session:
        session.execute(text("DELETE FROM simple_table WHERE id = :id"), {'id': record.id})
        session.commit()
    oracle_helper.clean_up()


def test_select_all_integration():
    oracle_helper = try_oracle_connection()
    if oracle_helper is None:
        return

    records = [
        SimpleTable(id=14, name="Test Name 2", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=True, salary=2000.00, birth_date="1991-01-01", decimal_value=2345.67),
        SimpleTable(id=15, name="Test Name 3", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=False, salary=3000.00, birth_date="1992-01-01", decimal_value=3456.78)
    ]

    oracle_helper.insert_all(records)

    with Session(oracle_helper.engine) as session:
        for record in records:
            result = session.execute(text("SELECT * FROM simple_table WHERE id = :id"), {'id': record.id})
            row = result.fetchone()

            assert row is not None
            assert row[0] == record.id
            assert row[1] == record.name
            assert row[2] == datetime(2024, 10, 17, 12, 0, 0)
            assert row[3] == datetime(2024, 10, 17, 12, 0, 0)
            assert row[4] == int(record.is_active)  # Convertir en entier pour le test
            assert row[5] == record.salary
            assert row[6] == datetime(1991, 1, 1) if record.id == 2 else datetime(1992, 1, 1)
            assert row[7] == record.decimal_value

    res = oracle_helper.select_all(SimpleTable)
    assert res == records
    with Session(oracle_helper.engine) as session:
        session.execute(text("DELETE FROM simple_table WHERE id in (14, 15)"))
        session.commit()

    oracle_helper.clean_up()


def test_select_with_where_clause_integration():
    oracle_helper = try_oracle_connection()
    if oracle_helper is None:
        return

    records = [
        SimpleTable(id=16, name="Test Name 2", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=True, salary=2000.00, birth_date="1991-01-01", decimal_value=2345.67),
        SimpleTable(id=17, name="Test Name 3", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=False, salary=3000.00, birth_date="1992-01-01", decimal_value=3456.78)
    ]

    oracle_helper.insert_all(records)

    with Session(oracle_helper.engine) as session:
        for record in records:
            result = session.execute(text("SELECT * FROM simple_table WHERE id = :id"), {'id': record.id})
            row = result.fetchone()

            assert row is not None
            assert row[0] == record.id
            assert row[1] == record.name
            assert row[2] == datetime(2024, 10, 17, 12, 0, 0)
            assert row[3] == datetime(2024, 10, 17, 12, 0, 0)
            assert row[4] == int(record.is_active)  # Convertir en entier pour le test
            assert row[5] == record.salary
            assert row[6] == datetime(1991, 1, 1) if record.id == 2 else datetime(1992, 1, 1)
            assert row[7] == record.decimal_value

    res = oracle_helper.select_one(SimpleTable, where="name = 'Test Name 2' and id = 16")
    assert res == records[0]
    with Session(oracle_helper.engine) as session:
        session.execute(text("DELETE FROM simple_table WHERE id in (16, 17)"))
        session.commit()

    oracle_helper.clean_up()


def test_select_all_with_where_clause_integration():
    oracle_helper = try_oracle_connection()
    if oracle_helper is None:
        return

    records = [
        SimpleTable(id=18, name="Test Name WHERE CLAUSE 2", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=True, salary=2000.00, birth_date="1991-01-01", decimal_value=2345.67),
        SimpleTable(id=19, name="Test Name WHERE CLAUSE 2", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=False, salary=3000.00, birth_date="1992-01-01", decimal_value=3456.78),
        SimpleTable(id=20, name="Test Name 3", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=False, salary=3000.00, birth_date="1992-01-01", decimal_value=3456.78)
    ]

    oracle_helper.insert_all(records)

    with Session(oracle_helper.engine) as session:
        for record in records:
            result = session.execute(text("SELECT * FROM simple_table WHERE id = :id"), {'id': record.id})
            row = result.fetchone()

            assert row is not None
            assert row[0] == record.id
            assert row[1] == record.name
            assert row[2] == datetime(2024, 10, 17, 12, 0, 0)
            assert row[3] == datetime(2024, 10, 17, 12, 0, 0)
            assert row[4] == int(record.is_active)  # Convertir en entier pour le test
            assert row[5] == record.salary
            assert row[6] == datetime(1991, 1, 1) if record.id == 2 else datetime(1992, 1, 1)
            assert row[7] == record.decimal_value

    res = oracle_helper.select_all(SimpleTable, where="name = 'Test Name WHERE CLAUSE 2'")
    assert res == records[:2]
    with Session(oracle_helper.engine) as session:
        session.execute(text("DELETE FROM simple_table WHERE id in (18, 19, 20)"))
        session.commit()

    oracle_helper.clean_up()


def test_select_in_batches_integration():
    oracle_helper = try_oracle_connection()
    if oracle_helper is None:
        return

    records = [
        SimpleTable(id=21, name="Test Name WHERE CLAUSE 2", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=True, salary=2000.00, birth_date="1991-01-01", decimal_value=2345.67),
        SimpleTable(id=22, name="Test Name WHERE CLAUSE 2", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=False, salary=3000.00, birth_date="1992-01-01", decimal_value=3456.78),
        SimpleTable(id=23, name="Test Name 3", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=False, salary=3000.00, birth_date="1992-01-01", decimal_value=3456.78),
        SimpleTable(id=24, name="Test Name 3", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=False, salary=3000.00, birth_date="1992-01-01", decimal_value=3456.78),
        SimpleTable(id=25, name="Test Name 3", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=False, salary=3000.00, birth_date="1992-01-01", decimal_value=3456.78),
        SimpleTable(id=26, name="Test Name 3", created_at="2024-10-17 12:00:00", updated_at="2024-10-17 12:00:00",
                    is_active=False, salary=3000.00, birth_date="1992-01-01", decimal_value=3456.78)
    ]

    oracle_helper.insert_all(records)

    with Session(oracle_helper.engine) as session:
        for record in records:
            result = session.execute(text("SELECT * FROM simple_table WHERE id = :id"), {'id': record.id})
            row = result.fetchone()

            assert row is not None
            assert row[0] == record.id
            assert row[1] == record.name
            assert row[2] == datetime(2024, 10, 17, 12, 0, 0)
            assert row[3] == datetime(2024, 10, 17, 12, 0, 0)
            assert row[4] == int(record.is_active)  # Convertir en entier pour le test
            assert row[5] == record.salary
            assert row[6] == datetime(1991, 1, 1) if record.id == 2 else datetime(1992, 1, 1)
            assert row[7] == record.decimal_value

    res = oracle_helper.select_in_batches(SimpleTable, chunksize=2)
    assert next(res) == records[0:2]
    assert next(res) == records[2:4]
    assert next(res) == records[4:6]

    with Session(oracle_helper.engine) as session:
        session.execute(text("DELETE FROM simple_table WHERE id in (21, 22, 23, 24, 25, 26)"))
        session.commit()

    oracle_helper.clean_up()
