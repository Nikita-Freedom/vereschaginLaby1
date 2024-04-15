# script4.py

def load_csv(filepath):
    # Та же функция, что и в предыдущих скриптах
    ...

def convert_types(data):
    for record in data:
        record['amount'] = float(record['amount'])
        record['isFraud'] = int(record['isFraud'])
    return data

def map_fraud_transactions(data):
    return [(record['type'], record['amount']) for record in data if record['isFraud'] == 1]

def reduce_fraud_transactions(mapped_data):
    reduced_data = {}
    for key, value in mapped_data:
        reduced_data[key] = reduced_data.get(key, 0) + value
    return reduced_data

if __name__ == "__main__":
    filepath = 'C:/Users/Nikitaa/PycharmProjects/vereschaginLaby/Synthetic_Financial_datasets_log.csv'
    data = load_csv(filepath)
    data = convert_types(data)
    mapped_data = map_fraud_transactions(data)
    result = reduce_fraud_transactions(mapped_data)
    print(result)
