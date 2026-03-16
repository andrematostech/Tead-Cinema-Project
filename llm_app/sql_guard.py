import re

from schema_context import ALLOWED_TABLES


BLOCKED_SQL_WORDS = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "CREATE",
    "TRUNCATE",
    "MERGE",
    "GRANT",
    "REVOKE",
}


def normalize_sql(sql: str) -> str:
    return " ".join(sql.strip().split())


def ensure_single_statement(sql: str) -> None:
    text = sql.strip().rstrip(";")
    if ";" in text:
        raise ValueError("Only one SQL statement is allowed.")


def ensure_select_only(sql: str) -> None:
    first_word = normalize_sql(sql).split(" ", 1)[0].upper()
    if first_word != "SELECT":
        raise ValueError("Only SELECT queries are allowed.")


def ensure_blocked_words_absent(sql: str) -> None:
    upper_sql = sql.upper()
    for word in BLOCKED_SQL_WORDS:
        if re.search(rf"\b{word}\b", upper_sql):
            raise ValueError(f"Blocked SQL keyword found: {word}")


def ensure_only_allowed_tables(sql: str) -> None:
    allowed_full_names = {table["full_name"].lower() for table in ALLOWED_TABLES.values()}
    referenced_tables = {
        match.lower()
        for match in re.findall(
            r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_\.]*)",
            sql,
            flags=re.IGNORECASE,
        )
    }

    if not referenced_tables:
        raise ValueError("The query must reference an allowed Gold table.")

    for table_name in referenced_tables:
        if table_name not in allowed_full_names:
            raise ValueError(f"Table is not allowed: {table_name}")


def validate_sql(sql: str) -> str:
    cleaned_sql = normalize_sql(sql)

    ensure_single_statement(cleaned_sql)
    ensure_select_only(cleaned_sql)
    ensure_blocked_words_absent(cleaned_sql)
    ensure_only_allowed_tables(cleaned_sql)

    return cleaned_sql.rstrip(";")
