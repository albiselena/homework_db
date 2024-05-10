import json

import psycopg2

from config import config

from unidecode import unidecode


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'my_new_db'

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, json_file)
                print(f"FOREIGN KEY успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    params_copy = params.copy()
    params_copy.update({'dbname': 'postgres'})
    conn = psycopg2.connect(**params_copy)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE {db_name}")
    conn.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file, 'r') as file:
        cur.execute(file.read())
        cur.execute("""ALTER TABLE products ADD COLUMN supplier_id int;""")


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    cur.execute("CREATE TABLE suppliers (\n"
                "                    supplier_id SERIAL PRIMARY KEY,\n"
                "                    company_name VARCHAR(100) NOT NULL,\n"
                "                    contact VARCHAR(100) NOT NULL,\n"
                "                    phone VARCHAR(50) NOT NULL,\n"
                "                    fax VARCHAR(50) NOT NULL,\n"
                "                    address VARCHAR(255) NOT NULL\n"
                "                );")


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    with open(json_file, 'r') as file:
        suppliers = json.load(file)

    for supplier in suppliers:
        supplier['company_name'] = unidecode(supplier['company_name'])
        supplier['contact'] = unidecode(supplier['contact'])
        supplier['phone'] = unidecode(supplier['phone'])
        supplier['fax'] = unidecode(supplier['fax'])
        supplier['address'] = unidecode(supplier['address'])

    return suppliers


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    for supplier in suppliers:
        cur.execute("INSERT INTO suppliers (company_name, contact, phone, fax, address) "
                    "VALUES (%s, %s, %s, %s, %s);",
                    (supplier['company_name'], supplier['contact'], supplier['phone'], supplier['fax'],
                     supplier['address']))


def add_foreign_keys(cur, json_file) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    with open(json_file, 'r') as file:
        suppliers = json.load(file)

    for supplier in suppliers:
        cur.execute(
            f"UPDATE products SET supplier_id = {supplier['supplier_id']} WHERE supplier = '{supplier['company_name']}';")


if __name__ == '__main__':
    main()
