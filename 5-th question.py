# ВОПРОС 5.
# 5. Общая сумма и количество транзакций, отмеченных как транзакции, совершенные мошенническими агентами в
# рамках симуляции. (isFraud = 1). Это даст понимание эффективности системы флагов подозрительных операций.
import re

# Функция для загрузки данных из файла CSV
def load_csv(filepath):
    data = []
    expected_columns = {'step', 'type', 'amount', 'nameOrig', 'oldbalanceOrg', 'newbalanceOrig', 'nameDest',
                        'oldbalanceDest', 'newbalanceDest', 'isFraud', 'isFlaggedFraud'}
    with open(filepath, 'r') as file:
        headers = file.readline().strip().split(';')  # Читаем заголовки столбцов из первой строки
        header_set = set(headers)  # Преобразуем список заголовков в множество для проверки
        if not expected_columns.issubset(header_set):
            raise ValueError("Some required columns are missing")  # Проверяем наличие всех необходимых колонок

        for line in file:
            values = line.strip().split(';')  # Разделяем строки на значения
            if len(values) == len(headers):
                record = dict(zip(headers, values))  # Создаем словарь для каждой строки
                data.append(record)  # Добавляем словарь в список данных
            else:
                print("Skipping malformed line:", line)  # Пропускаем строки с ошибками
    return data

# Функция для конвертации типов данных
def convert_types(data):
    converted_data = []
    invalid_data_log = []  # Лог для хранения невалидных данных
    pattern = re.compile(r'^\d*\.?\d*$')  # Паттерн для проверки строк на числовой формат

    for record in data:
        amount_str = record['amount'].replace(',', '.')  # Преобразуем запятые в точки для правильного формата числа
        if pattern.match(amount_str):  # Проверяем, соответствует ли строка паттерну
            try:
                record['amount'] = float(amount_str)  # Конвертируем строку в число
                record['isFraud'] = int(record['isFraud'])  # Конвертируем флаг мошенничества в целочисленный тип
                converted_data.append(record)
            except ValueError:
                invalid_data_log.append(record)  # Добавляем невалидную запись в лог
        else:
            invalid_data_log.append(record)  # Добавляем невалидную запись в лог

    print(f"Total invalid records skipped: {len(invalid_data_log)}")
    return converted_data

# Map функция для фильтрации мошеннических транзакций
def map_fraud_transactions(data):
    return [(record['type'], record['amount']) for record in data if record['isFraud'] == 1]  # Собираем кортежи типа транзакции и суммы

# Reduce функция для подсчета общей суммы и количества мошеннических транзакций по типам
def reduce_fraud_transactions(mapped_data):
    reduced_data = {}
    for key, value in mapped_data:
        if key not in reduced_data:
            reduced_data[key] = {'total_amount': 0, 'count': 0}  # Инициализация ключа, если его нет
        reduced_data[key]['total_amount'] += value  # Суммируем суммы по типам
        reduced_data[key]['count'] += 1  # Считаем количество транзакций
    return reduced_data

# Основной блок выполнения
if __name__ == "__main__":
    filepath = 'C:/Users/Nikitaa/PycharmProjects/vereschaginLaby/Synthetic_Financial_datasets_log.csv'
    data = load_csv(filepath)  # Загрузка данных
    data = convert_types(data)  # Конвертация типов данных
    mapped_data = map_fraud_transactions(data)  # Применение Map функции
    result = reduce_fraud_transactions(mapped_data)  # Применение Reduce функции

    # Форматирование и вывод результатов
    formatted_result = {
        key: {'Total Amount': f"${values['total_amount']:,.2f}", 'Count': values['count']}
        for key, values in result.items()
    }
    print(formatted_result)


"""
Как работает алгоритм MapReduce в этом случае:
Map Function (map_fraud_transactions):

Эта функция проходит через каждую запись в данных и создаёт список кортежей, состоящий из типа операции (type) и суммы (amount), 
но только для тех записей, где isFraud == 1. Это позволяет собрать все мошеннические операции для последующего анализа.
Reduce Function (reduce_fraud_transactions):

Функция Reduce принимает все кортежи, сгенерированные функцией Map, и агрегирует их по типам транзакций. 
Для каждого типа транзакции подсчитывается общая сумма и количество мошеннических операций. 
Это позволяет определить, какие типы операций наиболее уязвимы к мошенничеству.

"""