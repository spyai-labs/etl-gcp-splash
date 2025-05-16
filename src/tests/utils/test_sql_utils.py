from splash.utils.sql_utils import quote_bq_identifier, generate_merge_sql, generate_update_sql

def test_quote_bq_identifier_simple():
    assert quote_bq_identifier("my_column") == "`my_column`"

def test_quote_bq_identifier_with_backtick():
    assert quote_bq_identifier("weird`name") == "`weird\\`name`"

def test_generate_merge_sql():
    result = generate_merge_sql(
        stg_table_id="project.dataset.stg_table",
        dest_table_id="project.dataset.dest_table",
        key_column="id",
        columns=["id", "name", "email"]
    )
    assert "MERGE `project.dataset.dest_table`" in result
    assert "UPDATE SET T.`name`=S.`name`, T.`email`=S.`email`" in result
    assert "INSERT (`id`, `name`, `email`)" in result
    assert "VALUES (S.`id`, S.`name`, S.`email`)" in result

def test_generate_update_sql():
    result = generate_update_sql(
        stg_table_id="project.dataset.stg_table",
        dest_table_id="project.dataset.dest_table",
        key_column="id"
    )
    assert "UPDATE `project.dataset.dest_table`" in result
    assert "SET _deleted = TRUE" in result
    assert "WHERE `id` NOT IN" in result
    assert "SELECT DISTINCT `id` FROM `project.dataset.stg_table`" in result
