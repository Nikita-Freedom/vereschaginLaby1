# ВОПРОС 1
# 1. Общая сумма транзакций по каждому типу операции.
# Поможет понять, какие типы операций наиболее популярны с точки зрения оборота денежных средств.

import re
import json

# Функция для загрузки данных из CSV файла
def load_csv(filepath):
    data = []
    expected_columns = {'step', 'type', 'amount', 'nameOrig', 'oldbalanceOrg', 'newbalanceOrig', 'nameDest',
                        'oldbalanceDest', 'newbalanceDest', 'isFraud', 'isFlaggedFraud'}
    with open(filepath, 'r') as file:
        headers = file.readline().strip().split(';')  # Чтение и разделение заголовков файла
        header_set = set(headers)
        if not expected_columns.issubset(header_set):
            raise ValueError("Some required columns are missing")  # Проверка наличия всех необходимых колонок

        for line in file:
            values = line.strip().split(';')  # Разделение строк на значения
            if len(values) == len(headers):
                record = dict(zip(headers, values))  # Сопоставление заголовков с данными
                data.append(record)  # Добавление записи в список данных
            else:
                print("Skipping malformed line:", line)  # Пропуск поврежденных строк
    return data

# Функция для конвертации типов данных в полях
def convert_types(data):
    converted_data = []
    invalid_data_log = []
    pattern = re.compile(r'^\d*\.?\d*$')  # Паттерн для проверки числового формата

    for record in data:
        amount_str = record['amount'].replace(',', '.')  # Корректировка формата числа (запятые на точки)
        if pattern.match(amount_str):
            record['amount'] = float(amount_str)  # Преобразование строки в число
            converted_data.append(record)  # Добавление корректно преобразованной записи
        else:
            invalid_data_log.append(record)  # Добавление некорректной записи в лог

    with open('invalid_data_log.json', 'w') as log_file:
        json.dump(invalid_data_log, log_file, ensure_ascii=False, indent=4)  # Сохранение лога в файл

    print(f"Total invalid records skipped: {len(invalid_data_log)}")  # Вывод количества пропущенных записей
    return converted_data

# Функция Map для группировки транзакций по типам и суммам
def map_transactions_by_type(data):
    return [(record['type'], record['amount']) for record in data]  # Создание списка кортежей (тип, сумма)

# Функция Reduce для подсчета общих сумм по каждому типу транзакций
def reduce_transactions_by_type(mapped_data):
    reduced_data = {}
    for key, value in mapped_data:
        reduced_data[key] = reduced_data.get(key, 0) + value  # Суммирование значений для каждого ключа
    return reduced_data

# Функция для форматирования суммы в денежный формат
def format_currency(value):
    return f"${value:,.2f}"

if __name__ == "__main__":
    filepath = 'C:/Users/Nikitaa/PycharmProjects/vereschaginLaby/Synthetic_Financial_datasets_log.csv'
    data = load_csv(filepath)
    data = convert_types(data)
    mapped_data = map_transactions_by_type(data)
    result = reduce_transactions_by_type(mapped_data)

    formatted_result = {key: format_currency(value) for key, value in result.items()}  # Форматирование и вывод результатов
    print(formatted_result)

"""
Как работает алгоритм MapReduce в этом случае:

Map (map_transactions_by_type): Эта функция проходит по каждой записи в данных и создаёт кортежи, где первый элемент — 
тип транзакции (type), а второй — сумма транзакции (amount). Это позволяет группировать данные по типу транзакции для последующего подсчета.

Reduce (reduce_transactions_by_type): Функция Reduce берет все кортежи, созданные функцией Map, и агрегирует их по ключам (типам транзакций). 
Для каждого типа транзакции суммируются все суммы, что позволяет получить общую сумму денежных средств, прошедших через каждый тип операции.

Это типичный пример использования MapReduce для агрегации данных, где Map предназначен для создания структурированного вида данных, 
а Reduce — для их суммирования по заданным критериям.

"""