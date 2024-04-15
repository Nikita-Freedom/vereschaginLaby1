# ВОПРОС 3
# 3. Средняя сумма транзакции по каждому типу операции.
# Такой вопрос поможет выявить, для каких операций характерны наибольшие денежные переводы.

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
    invalid_data_log = []  # Добавляем список для логирования невалидных данных
    for record in data:
        try:
            record['amount'] = float(record['amount'])
            converted_data.append(record)
        except ValueError:
            # Логируем невалидные данные
            invalid_data_log.append(record)
    print(f"Total invalid records skipped: {len(invalid_data_log)}")
    return converted_data

def map_average_transaction_amount(data):
    return [(record['type'], (record['amount'], 1)) for record in data]

def reduce_average_transaction_amount(mapped_data):
    sum_data = {}
    count_data = {}
    for key, value in mapped_data:
        if key not in sum_data:
            sum_data[key] = 0
            count_data[key] = 0
        sum_data[key] += value[0]
        count_data[key] += value[1]
    average_data = {key: sum_data[key] / count_data[key] for key in sum_data}
    return average_data

if __name__ == "__main__":
    filepath = 'C:/Users/Nikitaa/PycharmProjects/vereschaginLaby/Synthetic_Financial_datasets_log.csv'
    data = load_csv(filepath)
    data = convert_types(data)
    mapped_data = map_average_transaction_amount(data)
    result = reduce_average_transaction_amount(mapped_data)
    print(result)
