# ВОПРОС 2
# 2. Количество транзакций, совершённых каждым отправителем (nameOrig).
# Это даст представление о наиболее активных пользователях системы.

import re
import json

# Функция для загрузки данных из CSV файла
def load_csv(filepath):
    data = []
    expected_columns = {'step', 'type', 'amount', 'nameOrig', 'oldbalanceOrg', 'newbalanceOrig', 'nameDest',
                        'oldbalanceDest', 'newbalanceDest', 'isFraud', 'isFlaggedFraud'}
    with open(filepath, 'r') as file:
        headers = file.readline().strip().split(';')  # Читаем заголовки столбцов из первой строки
        header_set = set(headers)
        if not expected_columns.issubset(header_set):
            raise ValueError("Some required columns are missing")  # Проверяем наличие всех необходимых столбцов

        for line in file:
            values = line.strip().split(';')  # Разбиваем каждую строку на элементы
            if len(values) == len(headers):
                record = dict(zip(headers, values))  # Создаем словарь со значениями для каждой записи
                data.append(record)  # Добавляем запись в список данных
            else:
                print("Skipping malformed line:", line)  # Пропускаем строки с неправильным количеством данных
    return data

# Функция для конвертации типов данных в полях
def convert_types(data):
    converted_data = []
    invalid_data_log = []  # Список для хранения некорректных записей
    pattern = re.compile(r'^\d*\.?\d*$')  # Паттерн для проверки строк на наличие только чисел

    for record in data:
        amount_str = record['amount'].replace(',', '.')  # Заменяем запятые на точки для корректного преобразования в число
        if pattern.match(amount_str):
            record['amount'] = float(amount_str)  # Конвертируем строку в число с плавающей точкой
            converted_data.append(record)
        else:
            invalid_data_log.append(record)  # Сохраняем невалидные записи

    with open('invalid_data_log.json', 'w') as log_file:
        json.dump(invalid_data_log, log_file, ensure_ascii=False, indent=4)  # Записываем невалидные данные в файл

    print(f"Total invalid records skipped: {len(invalid_data_log)}")  # Выводим количество пропущенных записей
    return converted_data

# Map функция для создания списка транзакций по отправителям
def map_transactions_by_sender(data):
    return [(record['nameOrig'], 1) for record in data]  # Возвращаем кортеж с именем отправителя и счетчиком

# Reduce функция для подсчета количества транзакций по каждому отправителю
def reduce_transactions_by_sender(mapped_data):
    reduced_data = {}
    for key, value in mapped_data:
        reduced_data[key] = reduced_data.get(key, 0) + value  # Суммируем количество транзакций для каждого отправителя
    return reduced_data

# Основной блок выполнения
if __name__ == "__main__":
    filepath = 'C:/Users/Nikitaa/PycharmProjects/vereschaginLaby/Synthetic_Financial_datasets_log.csv'
    data = load_csv(filepath)  # Загружаем данные
    data = convert_types(data)  # Конвертируем типы данных
    mapped_data = map_transactions_by_sender(data)  # Применяем Map функцию
    result = reduce_transactions_by_sender(mapped_data)  # Применяем Reduce функцию
    print(result)  # Выводим результат


"""
Как работает алгоритм MapReduce в этом случае:

Map (map_transactions_by_sender): Функция проходит по каждой записи в списке данных и создает кортеж (nameOrig, 1), 
где nameOrig это идентификатор отправителя, а 1 это счетчик для каждой транзакции этого отправителя. 
Это позволяет подготовить данные для агрегации по каждому отправителю.

Reduce (reduce_transactions_by_sender): Функция принимает список кортежей, созданных функцией Map, 
и агрегирует их по ключу (имени отправителя). Для каждого отправителя суммируется количество транзакций, 
что позволяет получить итоговое количество транзакций, выполненных каждым пользователем.

Этот подход позволяет эффективно агрегировать данные по определенному критерию (в данном случае, по отправителям транзакций), 
что является классическим примером использования алгоритма MapReduce для анализа больших объемов данных.
"""