from ...pycache.sql import *
import pytest


def test_identifier():
    assert Identifier("user").to_string() == "`user`"
    assert Identifier("col`name").to_string() == "`col``name`"


def test_literal():
    assert Literal("test").to_string() == "'test'"
    assert Literal("it's fine").to_string() == "'it''s fine'"
    assert Literal(42).to_string() == "42"
    assert Literal(True).to_string() == "1"
    assert Literal(None).to_string() == "NULL"


def test_placeholder():
    assert Placeholder("?").to_string() == "?"
    assert Placeholder("%s").to_string() == "%s"
    assert Placeholder("%(user_id)s").to_string() == "%(user_id)s"


def test_composed_basic():
    query = Composed(
        [SQL("SELECT "), Identifier("name"), SQL(" FROM "), Identifier("users")]
    )
    expected = "SELECT `name` FROM `users`"
    assert query.to_string() == expected


def test_composed_with_literal():
    query = Composed(
        [
            SQL("INSERT INTO "),
            Identifier("users"),
            SQL(" ("),
            Identifier("name"),
            SQL(") VALUES ("),
            Literal("John"),
            SQL(")"),
        ]
    )
    expected = "INSERT INTO `users` (`name`) VALUES ('John')"
    assert query.to_string() == expected


def test_composed_with_placeholder():
    query = Composed(
        [
            SQL("SELECT * FROM "),
            Identifier("users"),
            SQL(" WHERE "),
            Identifier("id"),
            SQL(" = "),
            Placeholder("%s"),
        ]
    )
    expected = "SELECT * FROM `users` WHERE `id` = %s"
    assert query.to_string() == expected


# Additional comprehensive tests


def test_sql_constructor():
    """Test SQL constructor with valid and invalid inputs"""
    # Valid string
    sql = SQL("SELECT * FROM users")
    assert sql.to_string() == "SELECT * FROM users"

    # Empty string
    sql = SQL("")
    assert sql.to_string() == ""

    # Invalid type
    with pytest.raises(TypeError, match="SQL statement must be a string"):
        SQL(123)

    with pytest.raises(TypeError, match="SQL statement must be a string"):
        SQL(None)


def test_sql_format_basic():
    """Test SQL format method with basic placeholders"""
    sql = SQL("SELECT * FROM {table} WHERE {column} = {value}")

    result = sql.format(
        table=Identifier("users"), column=Identifier("id"), value=Literal(42)
    )

    expected = "SELECT * FROM `users` WHERE `id` = 42"
    assert result.to_string() == expected


def test_sql_format_with_sql_fragments():
    """Test SQL format method with SQL fragments"""
    sql = SQL("SELECT {columns} FROM {table}")

    result = sql.format(
        columns=Composed([Identifier("name"), SQL(", "), Identifier("email")]),
        table=Identifier("users"),
    )

    expected = "SELECT `name`, `email` FROM `users`"
    assert result.to_string() == expected


def test_sql_format_error_cases():
    """Test SQL format method error cases"""
    # Format specifier not allowed
    with pytest.raises(TypeError, match="Non sql standard"):
        SQL("SELECT {name:10} FROM users").format(name=Identifier("test"))

    # Conversion not allowed
    with pytest.raises(TypeError, match="Non sql standard"):
        SQL("SELECT {name!r} FROM users").format(name=Identifier("test"))

    # Numeric placeholder not allowed
    with pytest.raises(TypeError, match="Format placeholder can't be string"):
        SQL("SELECT {0} FROM users").format(Identifier("test"))


def test_identifier_constructor():
    """Test Identifier constructor with valid and invalid inputs"""
    # Valid string
    ident = Identifier("user_name")
    assert ident.to_string() == "`user_name`"

    # Empty string
    ident = Identifier("")
    assert ident.to_string() == "``"

    # Invalid type
    with pytest.raises(TypeError, match="Identifer must be a string"):
        Identifier(123)

    with pytest.raises(TypeError, match="Identifer must be a string"):
        Identifier(None)


def test_identifier_escaping():
    """Test Identifier escaping of backticks"""
    # Single backtick
    assert Identifier("col`name").to_string() == "`col``name`"

    # Multiple backticks
    assert Identifier("col``name").to_string() == "`col````name`"


def test_literal_constructor():
    """Test Literal constructor with various types"""
    # String
    assert Literal("hello").to_string() == "'hello'"

    # Integer
    assert Literal(42).to_string() == "42"

    # Float
    assert Literal(3.14).to_string() == "3.14"

    # Boolean
    assert Literal(True).to_string() == "1"
    assert Literal(False).to_string() == "0"

    # None
    assert Literal(None).to_string() == "NULL"

    # List (should convert to string)
    assert Literal([1, 2, 3]).to_string() == "[1, 2, 3]"


def test_literal_string_escaping():
    """Test Literal string escaping"""
    # Single quote
    assert Literal("it's fine").to_string() == "'it''s fine'"

    # Multiple quotes
    assert Literal("it''s fine").to_string() == "'it''''s fine'"

    # Quote at start
    assert Literal("'hello").to_string() == "'''hello'"

    # Quote at end
    assert Literal("hello'").to_string() == "'hello'''"


def test_placeholder_constructor():
    """Test Placeholder constructor with valid and invalid inputs"""
    # Valid string
    ph = Placeholder("?")
    assert ph.to_string() == "?"

    # Different format
    ph = Placeholder("%s")
    assert ph.to_string() == "%s"

    # Invalid type
    with pytest.raises(TypeError, match="Placeholder format must be a string"):
        Placeholder(123)

    with pytest.raises(TypeError, match="Placeholder format must be a string"):
        Placeholder(None)


def test_composed_constructor():
    """Test Composed constructor with valid and invalid inputs"""
    # Valid list
    composed = Composed([SQL("SELECT "), Identifier("name")])
    assert composed.to_string() == "SELECT `name`"

    # Empty list
    composed = Composed([])
    assert composed.to_string() == ""

    # Invalid type in list
    with pytest.raises(TypeError, match="Not a type of 'Composable'"):
        Composed([SQL("SELECT "), "invalid"])

    with pytest.raises(TypeError, match="Not a type of 'Composable'"):
        Composed([SQL("SELECT "), 123])


def test_composed_addition():
    """Test Composed addition with various types"""
    # Composed + Composable
    c1 = Composed([SQL("SELECT "), Identifier("name")])
    c2 = SQL(" FROM ")
    result = c1 + c2
    assert result.to_string() == "SELECT `name` FROM "

    # Composed + Composed
    c1 = Composed([SQL("SELECT "), Identifier("name")])
    c2 = Composed([SQL(" FROM "), Identifier("users")])
    result = c1 + c2
    assert result.to_string() == "SELECT `name` FROM `users`"

    # Composable + Composed
    sql = SQL("SELECT ")
    composed = Composed([Identifier("name"), SQL(" FROM "), Identifier("users")])
    result = sql + composed
    assert result.to_string() == "SELECT `name` FROM `users`"


def test_composable_addition():
    """Test Composable addition with various types"""
    # SQL + SQL
    sql1 = SQL("SELECT ")
    sql2 = SQL("name")
    result = sql1 + sql2
    assert result.to_string() == "SELECT name"

    # SQL + Composed
    sql = SQL("SELECT ")
    composed = Composed([Identifier("name"), SQL(" FROM "), Identifier("users")])
    result = sql + composed
    assert result.to_string() == "SELECT `name` FROM `users`"

    # Invalid addition
    sql = SQL("SELECT ")
    with pytest.raises(NotImplementedError):
        sql + "invalid"


def test_complex_query_building():
    """Test building complex queries using all components"""
    # Complex SELECT with WHERE and ORDER BY
    query = Composed(
        [
            SQL("SELECT "),
            Composed([Identifier("name"), SQL(", "), Identifier("email")]),
            SQL(" FROM "),
            Identifier("users"),
            SQL(" WHERE "),
            Identifier("age"),
            SQL(" > "),
            Literal(18),
            SQL(" AND "),
            Identifier("active"),
            SQL(" = "),
            Literal(True),
            SQL(" ORDER BY "),
            Identifier("name"),
        ]
    )

    expected = "SELECT `name`, `email` FROM `users` WHERE `age` > 18 AND `active` = 1 ORDER BY `name`"
    assert query.to_string() == expected


def test_sql_format_complex():
    """Test SQL format with complex nested structures"""
    sql = SQL("SELECT {columns} FROM {table} WHERE {condition}")

    columns = Composed(
        [
            Identifier("id"),
            SQL(", "),
            Identifier("name"),
            SQL(", "),
            Identifier("email"),
        ]
    )

    condition = Composed([Identifier("status"), SQL(" = "), Literal("active")])

    result = sql.format(columns=columns, table=Identifier("users"), condition=condition)

    expected = "SELECT `id`, `name`, `email` FROM `users` WHERE `status` = 'active'"
    assert result.to_string() == expected


def test_edge_cases():
    """Test various edge cases"""
    # Empty components
    empty_composed = Composed([])
    assert empty_composed.to_string() == ""

    # Single component
    single = Composed([SQL("test")])
    assert single.to_string() == "test"

    # Very long identifier
    long_ident = Identifier("very_long_table_name_with_many_characters")
    assert long_ident.to_string() == "`very_long_table_name_with_many_characters`"

    # Special characters in literal
    special_literal = Literal("line\nbreak\tand\ttab")
    assert special_literal.to_string() == "'line\nbreak\tand\ttab'"

    # Zero values
    assert Literal(0).to_string() == "0"
    assert Literal(0.0).to_string() == "0.0"


def test_simple_upsert_mysql_and_sqlite():
    """
    Test simple UPSERT query generation without joins.
    """
    # MySQL UPSERT
    mysql_query = Composed(
        [
            SQL("INSERT INTO "),
            Identifier("users"),
            SQL(" ("),
            Identifier("id"),
            SQL(", "),
            Identifier("name"),
            SQL(") VALUES ("),
            Placeholder("%s"),
            SQL(", "),
            Placeholder("%s"),
            SQL(") ON DUPLICATE KEY UPDATE "),
            Identifier("name"),
            SQL(" = VALUES("),
            Identifier("name"),
            SQL(")"),
        ]
    )

    expected_mysql = (
        "INSERT INTO `users` (`id`, `name`) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
    )
    assert mysql_query.to_string() == expected_mysql

    # SQLite UPSERT
    sqlite_query = Composed(
        [
            SQL("INSERT INTO "),
            Identifier("users"),
            SQL(" ("),
            Identifier("id"),
            SQL(", "),
            Identifier("name"),
            SQL(") VALUES ("),
            Placeholder("?"),
            SQL(", "),
            Placeholder("?"),
            SQL(") ON CONFLICT("),
            Identifier("id"),
            SQL(") DO UPDATE SET "),
            Identifier("name"),
            SQL(" = excluded."),
            Identifier("name"),
        ]
    )

    expected_sqlite = (
        "INSERT INTO `users` (`id`, `name`) VALUES (?, ?) "
        "ON CONFLICT(`id`) DO UPDATE SET `name` = excluded.`name`"
    )
    assert sqlite_query.to_string() == expected_sqlite
