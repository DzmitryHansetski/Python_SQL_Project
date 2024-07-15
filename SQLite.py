import sqlite3

import pandas as pd

connection = sqlite3.Connection('SQLite.db')
cursor = connection.cursor()

# Создаем таблицы EMP, CLIENTS, SALES согласно их структуры, указанной в условии задачи
cursor.execute('create table EMP (ROW_ID int, F_NAME text, L_NAME text)')
cursor.execute('create table CLIENTS (ROW_ID int, NAME text, EMP_ID int)')
cursor.execute('create table SALES (DATE text, AMOUNT int, CLIENTS_ID int, STATUS text)')

# Производим заполнение таблиц EMP, CLIENTS и SALES
cursor.execute('insert into EMP (ROW_ID, F_NAME, L_NAME)'
                        'values (1, "Иван", "Иванов"),'
                                '(2, "Петр", "Петров"),'
                                '(3, "Василий", "Васильев"),'
                                '(4, "Кирилл", "Кириллов")')

cursor.execute('insert into CLIENTS (ROW_ID, NAME, EMP_ID)'
                        'values (1, "ОАО Рога и со.", 2),'
                                '(2, "ЗАО Фирма", 4),'
                                '(3, "ЧТУП Статистика", 2),'
                                '(4, "ЗАО Фантастика", 1),'
                                '(5, "ОАО Импорт", 2),'
                                '(6, "ИП Кролик И.П.", 2),'
                                '(7, "ИП Заяц З.А.", 1)')

SALES_DATA = [
    ("19-10-2024", 38, 3, "Ожидает оплаты"),
    ("13-05-2024", 23, 6, "Оплачено"),
    ("27-02-2024", 31, 2, "Оплачено"),
    ("27-09-2024", 29, 3, "Ожидает оплаты"),
    ("23-02-2024", 16, 3, "Оплачено"),
    ("16-11-2024", 33, 7, "Ожидает оплаты"),
    ("01-01-2024", 16, 7, "Оплачено"),
    ("22-07-2024", 49, 6, "Ожидает оплаты"),
    ("01-09-2024", 31, 7, "Ожидает оплаты"),
    ("29-07-2024", 12, 1, "Ожидает оплаты"),
    ("11-09-2024", 6, 1, "Ожидает оплаты"),
    ("08-10-2024", 38, 2, "Ожидает оплаты"),
    ("10-10-2024", 44, 1, "Ожидает оплаты"),
    ("03-04-2024", 41, 3, "Оплачено"),
    ("10-10-2024", 40, 6, "Ожидает оплаты"),
    ("04-09-2024", 25, 2, "Ожидает оплаты"),
    ("18-11-2024", 39, 2, "Ожидает оплаты"),
    ("21-07-2024", 40, 2, "Ожидает оплаты"),
    ("27-09-2024", 1, 4, "Ожидает оплаты"),
    ("05-03-2024", 33, 3, "Оплачено"),
    ("01-10-2024", 23, 4, "Ожидает оплаты"),
    ("26-07-2024", 50, 6, "Ожидает оплаты"),
    ("14-11-2024", 23, 7, "Ожидает оплаты"),
    ("17-02-2024", 24, 6, "Оплачено"),
    ("03-04-2024", 8, 6, "Оплачено"),
    ("19-03-2024", 31, 3, "Оплачено"),
    ("09-02-2024", 8, 2, "Оплачено"),
    ("08-07-2024", 47, 6, "Ожидает оплаты"),
    ("11-05-2024", 47, 3, "Оплачено"),
    ("27-04-2024", 5, 7, "Оплачено")
]

cursor.executemany('insert into SALES (DATE, AMOUNT, CLIENTS_ID, STATUS) values (?, ?, ?, ?)', SALES_DATA)

# Если необходимо, то производим удаление данных из таблиц EMP, CLIENTS или SALES
# Например, данной командой удаляются все данные из таблицы SALES по столбцу DATE
# cursor.execute('delete from SALES where DATE')

# 1. Выводим все данные по сотрудникам с наибольшей реализацией товара за одну продажу.
cursor.execute('''
    SELECT EMP.F_NAME, EMP.L_NAME, MAX(SALES.AMOUNT) as MAX_AMOUNT
    FROM SALES
    JOIN CLIENTS ON SALES.CLIENTS_ID = CLIENTS.ROW_ID
    JOIN EMP ON CLIENTS.EMP_ID = EMP.ROW_ID
    GROUP BY EMP.ROW_ID
    ORDER BY MAX_AMOUNT DESC
    LIMIT 1''')
print(cursor.fetchone())

# 2. Выводим наименование организации с наименьшим количеством заказов за все время.
cursor.execute('''
    SELECT CLIENTS.NAME, COUNT(SALES.CLIENTS_ID) as ORDER_COUNT
    FROM SALES
    JOIN CLIENTS ON SALES.CLIENTS_ID = CLIENTS.ROW_ID
    GROUP BY CLIENTS.ROW_ID
    ORDER BY ORDER_COUNT ASC
    LIMIT 1''')
print(cursor.fetchone())

# 3. Выводим общее количество заказов за месяц в разбивке по статусу.
# Извлекаем из таблицы SALES данные столбцов DATE, AMOUNT, CLIENTS_ID, STATUS и
# преобразовываем их в списки (type list) для дальнейшего создания датафрейма data = {}
cursor.execute('''SELECT DATE FROM SALES''')
DATE_SALES = cursor.fetchall()
res_DATE = []
for i in DATE_SALES:
    res_DATE += i if isinstance(i, tuple) else [i]
# print(res_DATE)

cursor.execute('''SELECT AMOUNT FROM SALES''')
AMOUNT_SALES = cursor.fetchall()
res_AMOUNT = []
for i in AMOUNT_SALES:
    res_AMOUNT += i if isinstance(i, tuple) else [i]
# print(res_AMOUNT)

cursor.execute('''SELECT CLIENTS_ID FROM SALES''')
CLIENTS_SALES = cursor.fetchall()
res_CLIENTS_ID = []
for i in CLIENTS_SALES:
    res_CLIENTS_ID += i if isinstance(i, tuple) else [i]
# print(res_CLIENTS_ID)

cursor.execute('''SELECT STATUS FROM SALES''')
STATUS_SALES = cursor.fetchall()
res_STATUS = []
for i in STATUS_SALES:
    res_STATUS += i if isinstance(i, tuple) else [i]
# print(res_STATUS)

# Для дальнейшего решения используем библиотеку pandas
# Создадим датафрейм data = {} на основе информации из таблицы SALES
data = {
    'DATE': res_DATE,
    'AMOUNT': res_AMOUNT,
    'CLIENTS_ID': res_CLIENTS_ID,
    'STATUS': res_STATUS
}

# Преобразуем словарь в DataFrame
df = pd.DataFrame(data)

# Преобразуем столбец DATE в формат datetime
df['DATE'] = pd.to_datetime(df['DATE'])

# Добавим столбец 'MONTH' для группировки по месяцам
df['MONTH'] = df['DATE'].dt.to_period('M')

# Сгруппируем данные по месяцу MONTH и статусу STATUS, и посчитаем количество заказов COUNT
result = df.groupby(['MONTH', 'STATUS']).size()

# Преобразуем в DataFrame для лучшего визуального представления результата
result = result.reset_index(name='COUNT')

# Выводим результат
print(result)

# 4. Подсчитаем общее количество оплаченных единиц товара с начала года.
query = '''
    SELECT strftime('%m-%Y', DATE) AS month,
       SUM(AMOUNT) AS monthly_amount,
       SUM(SUM(AMOUNT)) OVER (ORDER BY strftime('%m-%Y', DATE)) AS cumulative_amount
    FROM SALES
    WHERE STATUS = 'Оплачено'
    GROUP BY strftime('%m-%Y', DATE)
    ORDER BY month
'''

# Выполняем запрос и выводим результат
cursor.execute(query)
results = cursor.fetchall()

print("Месяц |   Продано всего   | Накапливаемый итог с начала года")
print("-------------------------------------------------------------")
for row in results:
    print(f"{row[0]}  | {row[1]:>17} | {row[2]:>17}")

connection.commit()
cursor.close()