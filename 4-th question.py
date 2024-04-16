# ВОПРОС 4
# 4. Количество и общая сумма мошеннических операций (isFraud = 1) по типам транзакций.
# Это позволит оценить, какие типы операций наиболее уязвимы для мошенничества.
import re

# Функция для загрузки данных из файла CSV
def load_csv(filepath):
    data = []
    expected_columns = {'step', 'type', 'amount', 'nameOrig', 'oldbalanceOrg', 'newbalanceOrig', 'nameDest',
                        'oldbalanceDest', 'newbalanceDest', 'isFraud', 'isFlaggedFraud'}
    with open(filepath, 'r') as file:
        headers = file.readline().strip().split(';')  # Читаем заголовки из первой строки
        header_set = set(headers)  # Преобразуем заголовки в множество для удобства проверки
        if not expected_columns.issubset(header_set):
            raise ValueError("Some required columns are missing")  # Проверяем, все ли необходимые колонки присутствуют

        for line in file:
            values = line.strip().split(';')  # Разделяем строку на значения
            if len(values) == len(headers):
                record = dict(zip(headers, values))  # Создаем словарь {заголовок: значение}
                data.append(record)  # Добавляем словарь в список данных
            else:
                print("Skipping malformed line:", line)  # Пропускаем строки с неправильным количеством данных
    return data

# Функция для конвертации строковых значений в числовые типы
def convert_types(data):
    converted_data = []
    invalid_data_log = []  # Список для хранения невалидных данных
    pattern = re.compile(r'^\d*\.?\d*$')  # Регулярное выражение для проверки, является ли строка числом

    for record in data:
        amount_str = record['amount'].replace(',', '.')  # Замена запятых точками для корректного преобразования в число
        if pattern.match(amount_str):  # Проверка соответствия строки регулярному выражению
            try:
                record['amount'] = float(amount_str)  # Конвертация строки в число
                record['isFraud'] = int(record['isFraud'])  # Конвертация флага мошенничества в число
                converted_data.append(record)
            except ValueError:
                invalid_data_log.append(record)  # Запись невалидных данных в лог
        else:
            invalid_data_log.append(record)

    print(f"Total invalid records skipped: {len(invalid_data_log)}")  # Вывод количества пропущенных записей
    return converted_data

# Map функция для фильтрации мошеннических транзакций и создания пар (тип транзакции, сумма)
def map_fraud_transactions(data):
    return [(record['type'], record['amount']) for record in data if record['isFraud'] == 1]

# Reduce функция для подсчета общей суммы по каждому типу мошеннических транзакций
def reduce_fraud_transactions(mapped_data):
    reduced_data = {}
    for key, value in mapped_data:
        reduced_data[key] = reduced_data.get(key, 0) + value  # Суммирование сумм по ключам (типам транзакций)
    return reduced_data

# Функция для форматирования числовых значений в денежный формат
def format_currency(value):
    return f"${value:,.2f}"

if __name__ == "__main__":
    filepath = 'C:/Users/Nikitaa/PycharmProjects/vereschaginLaby/Synthetic_Financial_datasets_log.csv'
    data = load_csv(filepath)  # Загрузка данных
    data = convert_types(data)  # Конвертация типов данных
    mapped_data = map_fraud_transactions(data)  # Применение Map функции
    result = reduce_fraud_transactions(mapped_data)  # Применение Reduce функции

    formatted_result = {key: format_currency(value) for key, value in result.items()}  # Форматирование результатов
    print(formatted_result)  # Вывод отформатированных результатов

"""
Как работает алгоритм MapReduce в этом случае:

Map (map_fraud_transactions):

Функция map_fraud_transactions проходит по всем записям и создает список кортежей. 
Каждый кортеж состоит из типа транзакции (type) и суммы транзакции (amount), но только для тех записей, где isFraud == 1. 
Таким образом, на выходе мы получаем данные только по мошенническим транзакциям.

Reduce (reduce_fraud_transactions):

Функция reduce_fraud_transactions принимает список кортежей, созданных функцией Map. 
Она итерирует по этим кортежам и суммирует суммы транзакций для каждого типа операции. 
Это позволяет получить общую сумму мошеннических операций для каждого типа транзакции. 
Reduce агрегирует данные по ключу (тип транзакции), суммируя значения (суммы транзакций).
Эта реализация алгоритма MapReduce позволяет эффективно обрабатывать и агрегировать данные, 
выделяя особенно важные аспекты (в данном случае, мошеннические транзакции), что крайне полезно для анализа и выявления уязвимостей в различных типах операций.

"""