# ВОПРОС 5.
# 5. Общая сумма и количество транзакций, отмеченных как транзакции, совершенные мошенническими агентами в
# рамках симуляции. (isFraud = 1). Это даст понимание эффективности системы флагов подозрительных операций.
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
    invalid_data_log = []
    pattern = re.compile(r'^\d*\.?\d*$')

    for record in data:
        amount_str = record['amount'].replace(',', '.')  # European format
        if pattern.match(amount_str):
            try:
                record['amount'] = float(amount_str)
                record['isFraud'] = int(record['isFraud'])
                converted_data.append(record)
            except ValueError:
                invalid_data_log.append(record)
        else:
            invalid_data_log.append(record)

    print(f"Total invalid records skipped: {len(invalid_data_log)}")
    return converted_data

def map_fraud_transactions(data):
    return [(record['type'], record['amount']) for record in data if record['isFraud'] == 1]

def reduce_fraud_transactions(mapped_data):
    reduced_data = {}
    for key, value in mapped_data:
        if key not in reduced_data:
            reduced_data[key] = {'total_amount': 0, 'count': 0}
        reduced_data[key]['total_amount'] += value
        reduced_data[key]['count'] += 1
    return reduced_data

if __name__ == "__main__":
    filepath = 'C:/Users/Nikitaa/PycharmProjects/vereschaginLaby/Synthetic_Financial_datasets_log.csv'
    data = load_csv(filepath)
    data = convert_types(data)
    mapped_data = map_fraud_transactions(data)
    result = reduce_fraud_transactions(mapped_data)

    formatted_result = {
        key: {'Total Amount': f"${values['total_amount']:,.2f}", 'Count': values['count']}
        for key, values in result.items()
    }
    print(formatted_result)
