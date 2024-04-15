# ВОПРОС 4
# 4. Количество и общая сумма мошеннических операций (isFraud = 1) по типам транзакций.
# Это позволит оценить, какие типы операций наиболее уязвимы для мошенничества.
import re

def load_csv(filepath):
    data = []
    expected_columns = {'step', 'type', 'amount', 'nameOrig', 'oldbalanceOrg', 'newbalanceOrig', 'nameDest',
                        'oldbalanceDest', 'newbalanceDest', 'isFraud', 'isFlaggedFraud'}
    with open(filepath, 'r') as file:
        headers = file.readline().strip().split(';')
        header_set = set(headers)
        if not expected_columns.issubset(header_set):
            raise ValueError("Some required columns are missing")

        for line in file:
            values = line.strip().split(';')
            if len(values) == len(headers):
                record = dict(zip(headers, values))
                data.append(record)
            else:
                print("Skipping malformed line:", line)
    return data


def convert_types(data):
    converted_data = []
    invalid_data_log = []  # Список для сохранения невалидных данных
    pattern = re.compile(r'^\d*\.?\d*$')  # Паттерн для проверки на числовой формат

    for record in data:
        amount_str = record['amount'].replace(',', '.')  # Замена запятых на точки для европейского формата
        if pattern.match(amount_str):
            try:
                record['amount'] = float(amount_str)
                record['isFraud'] = int(record['isFraud'])
                converted_data.append(record)
            except ValueError:
                invalid_data_log.append(record)  # Добавление записи в лог невалидных данных
        else:
            invalid_data_log.append(record)  # Добавление записи в лог невалидных данных

    print(f"Total invalid records skipped: {len(invalid_data_log)}")
    return converted_data


def map_fraud_transactions(data):
    return [(record['type'], record['amount']) for record in data if record['isFraud'] == 1]

def reduce_fraud_transactions(mapped_data):
    reduced_data = {}
    for key, value in mapped_data:
        reduced_data[key] = reduced_data.get(key, 0) + value
    return reduced_data

def format_currency(value):
    return f"${value:,.2f}"  # Форматирует число как денежное значение в долларах, с двумя десятичными знаками и разделителями тысяч


if __name__ == "__main__":
    filepath = 'C:/Users/Nikitaa/PycharmProjects/vereschaginLaby/Synthetic_Financial_datasets_log.csv'
    data = load_csv(filepath)
    data = convert_types(data)
    mapped_data = map_fraud_transactions(data)
    result = reduce_fraud_transactions(mapped_data)

    # Форматируем и печатаем результаты
    formatted_result = {key: format_currency(value) for key, value in result.items()}
    print(formatted_result)
