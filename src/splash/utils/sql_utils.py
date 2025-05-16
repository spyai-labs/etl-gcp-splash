from textwrap import dedent
from typing import List


def quote_bq_identifier(identifier: str) -> str:
    """
    Quotes a BigQuery identifier using backticks, escaping any existing backticks within.

    Args:
        identifier (str): The identifier to quote.

    Returns:
        str: Properly escaped and quoted BigQuery identifier.
    """
    escaped = identifier.replace("`", "\\`")
    return f"`{escaped}`"


def generate_merge_sql(
    stg_table_id: str,
    dest_table_id: str,
    key_column: str,
    columns: List[str]
) -> str:
    """
    Generates a BigQuery MERGE SQL statement for updating and inserting rows
    from a staging table into a destination table.

    Args:
        stg_table_id (str): Staging table full ID.
        dest_table_id (str): Destination table full ID.
        key_column (str): Primary key column for comparison.
        columns (List[str]): List of all columns in the table.

    Returns:
        str: A formatted BigQuery MERGE SQL string.
    """
    updates = ", ".join(f"T.{quote_bq_identifier(col)}=S.{quote_bq_identifier(col)}" for col in columns if col != key_column)
    inserts = ", ".join(quote_bq_identifier(col) for col in columns)
    insert_values = ", ".join(f"S.{quote_bq_identifier(col)}" for col in columns)
    
    merge_sql = f"""
    MERGE {quote_bq_identifier(dest_table_id)} T
    USING (SELECT * FROM {quote_bq_identifier(stg_table_id)}) S
    ON T.{quote_bq_identifier(key_column)} = S.{quote_bq_identifier(key_column)}
    WHEN MATCHED THEN
        UPDATE SET {updates}
    WHEN NOT MATCHED THEN
        INSERT ({inserts})
        VALUES ({insert_values});
    """
    
    return dedent(merge_sql).strip()


def generate_update_sql(
    stg_table_id: str,
    dest_table_id: str,
    key_column: str
) -> str:
    """
    Generates a BigQuery UPDATE SQL statement to mark records in the destination table
    as deleted if their key is not found in the staging table.

    Args:
        stg_table_id (str): Staging table full ID.
        dest_table_id (str): Destination table full ID.
        key_column (str): Key column used for exclusion join.

    Returns:
        str: A formatted BigQuery UPDATE SQL string.
    """
    update_sql = f"""
    UPDATE {quote_bq_identifier(dest_table_id)}
    SET _deleted = TRUE
    WHERE {quote_bq_identifier(key_column)} NOT IN (SELECT DISTINCT {quote_bq_identifier(key_column)} FROM {quote_bq_identifier(stg_table_id)});
    """
    return dedent(update_sql).strip()
