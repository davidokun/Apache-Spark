"""
Customer data generator
"""
import random


def generate_mock_data():
    """
    Generates mock data for customer's money spent
    """
    customers = ['David', 'Adam', 'Ana', 'Julia', 'Andres', 'Catalina', 'Charles', 'Sophia', 'Nick', 'Natasha']
    data = open('./files/customers_data.csv', mode='w')

    for i in range(1000):
        for c in customers:
            data.write(f"{c}, {random.randrange(1000, 50000)}\n")


if __name__ == '__main__':
    generate_mock_data()
