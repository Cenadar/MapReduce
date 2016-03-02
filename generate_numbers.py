import os
import random
import shutil


def generate_numbers(min_value, max_value, total_count, file_name):
    sum = 0

    with open(file_name, 'w') as file:
        for i in range(total_count):
            number = random.randint(min_value, max_value)
            sum += number
            file.write('{}\n'.format(number))

    with open('data/answer.txt', 'w') as answer:
        answer.write('{}'.format(sum))


def main():
    max_val = 1000000000
    total_count = 100000

    shutil.rmtree('data')
    os.mkdir('data')
    os.mkdir('data/input')
    file_name = 'data/input/0000.txt'

    generate_numbers(0, max_val, total_count, file_name)


if __name__ == '__main__':
    main()
