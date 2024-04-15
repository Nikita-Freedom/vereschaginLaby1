# ВОПРОС 2
# 2. Количество транзакций, совершённых каждым отправителем (nameOrig).
# Это даст представление о наиболее активных пользователях системы.

import re
import json

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
    invalid_data_log = []
    pattern = re.compile(r'^\d*\.?\d*$')  # Расширен для включения чисел без десятичной точки

    for record in data:
        amount_str = record['amount'].replace(',', '.')  # Замена запятых на точки
        if pattern.match(amount_str):
            record['amount'] = float(amount_str)
            converted_data.append(record)
        else:
            invalid_data_log.append(record)

    with open('invalid_data_log.json', 'w') as log_file:
        json.dump(invalid_data_log, log_file, ensure_ascii=False, indent=4)

    print(f"Total invalid records skipped: {len(invalid_data_log)}")
    return converted_data


def map_transactions_by_sender(data):
    return [(record['nameOrig'], 1) for record in data]

def reduce_transactions_by_sender(mapped_data):
    reduced_data = {}
    for key, value in mapped_data:
        reduced_data[key] = reduced_data.get(key, 0) + value
    return reduced_data

if __name__ == "__main__":
    filepath = 'C:/Users/Nikitaa/PycharmProjects/vereschaginLaby/Synthetic_Financial_datasets_log.csv'
    data = load_csv(filepath)
    data = convert_types(data)
    mapped_data = map_transactions_by_sender(data)
    result = reduce_transactions_by_sender(mapped_data)
    print(result)
