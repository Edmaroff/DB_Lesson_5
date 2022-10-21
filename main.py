import psycopg2
import configparser


# Удаление таблиц phone и client_info
def drop_table(connection, cursor):
    cursor.execute('''
        DROP TABLE IF EXISTS phone;
        DROP TABLE IF EXISTS client_info;
    ''')
    connection.commit()
    return print('Таблицы удалены.')


# Проверка, есть ли клиент с client_id в таблице
def check_info_by_client_id(cursor, client_id):
    cursor.execute('''
        SELECT * FROM client_info
        WHERE client_id = %s
    ''', (client_id,))
    return cursor.fetchall()


# Проверка есть ли клиент с email в таблице
def check_info_by_email(cursor, email):
    cursor.execute('''
        SELECT * FROM client_info
        WHERE email = %s
    ''', (email,))
    return cursor.fetchall()


# Проверка есть ли клиент с номером телефона в таблице
def check_info_by_phone(cursor, phone_number):
    cursor.execute('''
        SELECT * FROM phone
        WHERE phone_number = %s
    ''', (phone_number,))
    return cursor.fetchall()


# 1. Функция, создающая структуру БД (таблицы)
def create_table(connection, cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS client_info (
        client_id  INTEGER      PRIMARY KEY
                               GENERATED ALWAYS AS IDENTITY,
        first_name VARCHAR(20) NOT NULL,    
        surname    VARCHAR(30)    NOT NULL,
        email      VARCHAR(50)      UNIQUE NOT NULL
        );
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS phone (
        phone_id     INTEGER     PRIMARY KEY
                                 GENERATED ALWAYS AS IDENTITY,
        phone_number VARCHAR(12) UNIQUE,
        client_id    INTEGER     REFERENCES client_info(client_id)
        );
    ''')
    connection.commit()
    return print('Таблицы client_info и phone созданы.')


# 2. Функция, позволяющая добавить нового клиента
def add_new_client(cursor, first_name, surname, email, phone_number=None):
    if not check_info_by_email(cursor, email):
        cursor.execute('''
            INSERT INTO client_info(first_name, surname, email)
                 VALUES (%s, %s, %s) 
              RETURNING client_id, first_name, surname, email;
        ''', (first_name, surname, email))
        res = cur.fetchall()[0]
        if phone_number is not None:
            if not check_info_by_phone(cursor, phone_number):
                cursor.execute('''
                        INSERT INTO phone(phone_number, client_id)
                             VALUES (%s, %s)
                          RETURNING phone_number
                        ''', (phone_number, res[0]))
                res += cur.fetchone()
            else:
                return print(f'Клиент с номером = {phone_number} уже существует, добавить не получится.')
        return print(f'Добавили информацию о новом клиенте: {res}')
    else:
        return print(f'Клиент с email = {email} уже существует, добавить не получится.')


# 3. Функция, позволяющая добавить телефон для существующего клиента
def add_phone_number(cursor, client_id, phone_number):
    if check_info_by_client_id(cursor, client_id):
        if not check_info_by_phone(cursor, phone_number):
            cursor.execute('''
                INSERT INTO phone(phone_number, client_id)
                     VALUES (%s, %s)
                  RETURNING phone_number, client_id
            ''', (phone_number, client_id))
            res = cur.fetchone()
            return print(f'Добавили номер телефона: {res}')
        else:
            return print(f'Клиент с номером = {phone_number} уже существует, добавить номер не получится.')
    else:
        return print(f'Клиента с client_id = {client_id} не существует, добавить номер не получится.')


# 4. Функция, позволяющая изменить данные о клиенте
def change_client(connection, cursor, client_id, first_name, surname, email, phone_number=None, phone_id=None):
    if check_info_by_client_id(cursor, client_id):
        if not check_info_by_email(cursor, email):
            cursor.execute('''
                UPDATE client_info
                   SET first_name = %s, 
                       surname = %s, 
                       email = %s
                 WHERE client_id = %s;
            ''', (first_name, surname, email, client_id))
            connection.commit()
            if phone_number is not None and phone_id is not None:
                if not check_info_by_phone(cursor, phone_number):
                    cursor.execute('''
                        UPDATE phone
                           SET phone_number = %s 
                         WHERE client_id = %s
                           AND phone_id = %s;
                    ''', (phone_number, client_id, phone_id))
                    connection.commit()
                    return print('Указанные данные успешно обновлены')
                else:
                    return print(f'Клиент с номером = {phone_number} уже существует, изменить номер не получится.'
                                 f' Остальные данные были изменены.')
            else:
                return print('Указанные данные успешно обновлены.')
        else:
            return print(f'Клиент с email = {email} уже существует, изменить данные не получится.')
    else:
        return print(f'Клиента с client_id = {client_id} не существует, изменить данные не получится.')


# 5. Функция, позволяющая удалить телефон для существующего клиента
def delete_phone_number(connection, cursor, phone_number):
    if check_info_by_phone(cursor, phone_number):
        cursor.execute('''
            DELETE FROM phone 
             WHERE phone_number = %s;
        ''', (phone_number,))
        connection.commit()
        return print('Номер телефона удален.')
    else:
        return print(f'Клиента с номером = {phone_number} нет, удалить номер не получится.')


# 6. Функция, позволяющая удалить существующего клиента
def delete_client(connection, cursor, client_id):
    if check_info_by_client_id(cursor, client_id):
        cursor.execute('''
            DELETE FROM phone 
             WHERE client_id = %s;
        ''', (client_id,))
        cursor.execute('''
            DELETE FROM client_info 
             WHERE client_id = %s;
        ''', (client_id,))
        connection.commit()
        return print('Данные о клиенте удалены.')
    else:
        return print(f'Клиента с client_id = {client_id} не существует, удалить данные не получится.')


# 7. Функция, позволяющая найти клиента по его данным (имени, фамилии, email-у или телефону)
def find_client(cursor, first_name=None, surname=None, email=None, phone_number=None):
    if first_name is not None or surname is not None or email is not None:
        cursor.execute('''
            SELECT client_id, first_name
              FROM client_info
             WHERE first_name = %s
                OR surname = %s
                OR email = %s;
        ''', (first_name, surname, email))
        res = cur.fetchall()
        if not res:
            return print('Клиент с такими данными не найден.')
        else:
            return print(res)
    else:
        cursor.execute('''
                    SELECT client_info.client_id, first_name
                      FROM client_info 
                           JOIN phone 
                           ON client_info.client_id = phone.client_id
                     WHERE phone_number LIKE %s
                        ''', (phone_number,))
        res = cur.fetchall()
        if not res:
            return print('Клиент с такими номером не найден.')
        else:
            return print(res)


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("settings.ini")
    database = config['SQL']['database']
    user = config['SQL']['user']
    password = config['SQL']['password']
    with psycopg2.connect(database=database, user=user, password=password) as conn:
        with conn.cursor() as cur:
            # # Удаление и создание таблиц
            # drop_table(conn, cur)
            # create_table(conn, cur)
            #
            # # Проверка работы функции add_new_client
            # add_new_client(cur, 'Иван', 'Иванов', 'ivan@mail.ru')
            # add_new_client(cur, 'Иван', 'Петров', 'ivanpetrov@mail.ru')
            # add_new_client(cur, 'Петр', 'Петров', 'petr@mail.ru', '100')
            # add_new_client(cur, 'Сидр', 'Сидоров', 'ivan@mail.ru')
            # add_new_client(cur, 'Сидр', 'Сидоров', 'sidr@mail.ru', '100')
            # add_new_client(cur, 'Соболь', 'Соболев', 'sobol@mail.ru', '200')
            #
            # # Проверка работы функции add_phone_number
            # add_phone_number(cur, 1, '300')
            # add_phone_number(cur, 1, '400')
            # add_phone_number(cur, 1, '500')
            # add_phone_number(cur, 2, '300')
            # add_phone_number(cur, 3, '300')
            # add_phone_number(cur, 6, '600')
            #
            # # Проверка работы функции change_client
            # change_client(conn, cur, 1, 'Иван1', 'Иванов1', 'ivan@mail.ru1')
            # change_client(conn, cur, 1, 'Иван1', 'Иванов1', 'petr@mail.ru')
            # change_client(conn, cur, 6, 'Иван1', 'Иванов1', 'ivan@mail.ru1')
            # change_client(conn, cur, 6, 'Иван1', 'Иванов1', 'ivan@mail.ru11')
            # change_client(conn, cur, 1, 'Иван', 'Иванов', 'ivan@mail.ru', '700', 3)
            # change_client(conn, cur, 2, 'Петр1', 'Петров1', 'petr@mail.ru1', '700', 1)
            #
            # # Проверка работы функции delete_phone_number
            # delete_phone_number(conn, cur, '700')
            # delete_phone_number(conn, cur, '700')
            #
            # # Проверка работы функции delete_client
            # delete_client(conn, cur, 2)
            # delete_client(conn, cur, 2)
            #
            # # Проверка работы функции find_client
            # find_client(conn, cur, first_name='Соболь')
            # find_client(conn, cur, first_name='Иван')
            # find_client(conn, cur, surname='Петров1')
            # find_client(conn, cur, email='sobol@mail.ru')
            # find_client(conn, cur, first_name='Иван1')
            # find_client(conn, cur, phone_number='400')
            # find_client(conn, cur, phone_number='4000')

# Вопросы:
# 1. Можно ли в функциях drop_table и create_table передавать названия таблиц как параметры функции?
#       Не смог найти способ в интернете(
