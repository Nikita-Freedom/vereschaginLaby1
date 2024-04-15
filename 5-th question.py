# script5.py

def load_csv(filepath):
    # Та же функция, что и в предыдущих скриптах
    ...

def convert_types(data):
    for record in data:
        record['amount'] = float(record['amount'])
        record['isFlaggedFraud'] = int(record['isFlaggedFraud'])
    return data

def map_flagged_fraud_transactions(data):
    return [('flagged_fraud', (record['amount'], 1)) for record in data if record['isFlaggedFraud'] == 1]

def reduce_flagged_fraud_transactions(mapped_data):
    reduced_data = {'total_amount': 0, 'count': 0}
    for _, value in mapped_data:
        reduced_data['total_amount'] += value[0]
        reduced_data['count'] += value[1]
    return reduced_data

if __name__ == "__main__":
    filepath = 'C:/Users/Nikitaa/PycharmProjects/vereschaginLaby/Synthetic_Financial_datasets_log.csv'
    data = load_csv(filepath)
    data = convert_types(data)
    mapped_data = map_flagged_fraud_transactions(data)
    result = reduce_flagged_fraud_transactions(mapped_data)
    print(result)
