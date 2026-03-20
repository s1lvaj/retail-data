import duckdb
from pathlib import Path

DB_PATH = "warehouse.duckdb"
SQL_FILE = Path("sql/gold_models.sql")


def main():
    con = duckdb.connect(DB_PATH)

    sql = SQL_FILE.read_text()
    con.execute(sql)

    con.close()


if __name__ == "__main__":
    main()