# ВОПРОС 1
# 1. Средняя сумма транзакции по каждому типу операции.
# Поможет выявить, для каких операций характерны наибольшие денежные переводы.

def load_csv(filepath):
    data = []
    expected_columns = {'step', 'type', 'amount', 'nameOrig', 'oldbalanceOrg', 'newbalanceOrig', 'nameDest',
                        'oldbalanceDest', 'newbalanceDest', 'isFraud', 'isFlaggedFraud'}
    with open(filepath, 'r') as file:
        headers = file.readline().strip().split(';')  # Читаем заголовки из первой строки файла
        header_set = set(headers)
        if not expected_columns.issubset(header_set):
            raise ValueError("Some required columns are missing")  # Проверка наличия всех нужных колонок

        for line in file:
            values = line.strip().split(';')  # Разбиваем каждую строку на элементы
            if len(values) == len(headers):
                record = dict(zip(headers, values))  # Создаем словарь для каждой строки
                data.append(record)  # Добавляем словарь в список данных
            else:
                print("Skipping malformed line:", line)  # Пропуск некорректных строк
    return data

def convert_types(data):
    converted_data = []
    invalid_data_log = []  # Список для невалидных данных
    for record in data:
        try:
            record['amount'] = float(record['amount'])  # Преобразуем строку суммы в число
            converted_data.append(record)  # Добавляем запись в список конвертированных данных
        except ValueError:
            invalid_data_log.append(record)  # Добавляем запись в список невалидных данных
    print(f"Total invalid records skipped: {len(invalid_data_log)}")  # Вывод количества пропущенных записей
    return converted_data

def map_average_transaction_amount(data):
    # Создаем кортежи (тип транзакции, (сумма, счетчик))
    return [(record['type'], (record['amount'], 1)) for record in data]

def reduce_average_transaction_amount(mapped_data):
    sum_data = {}  # Словарь для сумм
    count_data = {}  # Словарь для счетчиков
    for key, value in mapped_data:
        if key not in sum_data:
            sum_data[key] = 0  # Инициализация суммы
            count_data[key] = 0  # Инициализация счетчика
        sum_data[key] += value[0]  # Добавление суммы
        count_data[key] += value[1]  # Инкремент счетчика
    # Вычисляем среднее значение, деля сумму на количество
    average_data = {key: sum_data[key] / count_data[key] for key in sum_data}
    return average_data

def format_currency(value):
    # Форматирование числа в виде денежной суммы
    return f"${value:,.2f}"

if __name__ == "__main__":
    filepath = 'C:/Users/Nikitaa/PycharmProjects/vereschaginLaby/Synthetic_Financial_datasets_log.csv'
    data = load_csv(filepath)  # Загружаем данные
    data = convert_types(data)  # Конвертируем типы данных
    mapped_data = map_average_transaction_amount(data)  # Применяем Map функцию
    result = reduce_average_transaction_amount(mapped_data)  # Применяем Reduce функцию

    formatted_result = {key: format_currency(value) for key, value in result.items()}  # Форматируем результат
    print(formatted_result)  # Выводим отформатированный результат

"""
Как работает алгоритм MapReduce в этом случае:

Map (map_average_transaction_amount):

Функция проходит по каждому элементу списка данных.
Для каждой записи создается кортеж вида (тип транзакции, (сумма транзакции, 1)). 
Второй элемент кортежа — это пара, где первый элемент — сумма транзакции, второй — счетчик транзакций (всегда равен 1),
 который будет использоваться для подсчета количества транзакций каждого типа.
 
Reduce (reduce_average_transaction_amount):

Функция принимает список кортежей, созданных функцией Map.
Использует два словаря (sum_data и count_data) для хранения промежуточных сумм и количества транзакций по каждому типу операции.
Для каждого кортежа из списка, функция увеличивает сумму и счетчик в соответствующих словарях.
После прохода по всем кортежам, функция вычисляет среднее значение для каждого типа операции, разделив общую сумму на количество транзакций.

"""