import base64
import functools
import multiprocessing
import os
import shutil
import pickle
import time
from reducer import SumReducer
from shuffler import Shuffler
from mapper import IdentityMapper
from sharding_function import DefaultShardingFunction
from file_emitter import FilesEmitter, FileEmitter


def key_value_decode(line):
    key, value = line.rstrip('\n').split(';')
    value = base64.b64decode(value)
    value = pickle.loads(value)
    return key, value


def map_file(file_name, mapper):
    for line_number, line in enumerate(open(file_name, 'r')):
        mapper.map(value=line, file_name=file_name, line_number=line_number)


def reduce_file(in_file_name, reducer):
    last_key = None
    values = []
    with open(in_file_name, 'r') as file:
        for line in file:
            key, value = key_value_decode(line)

            if key != last_key:
                if len(values) > 0:
                    reducer.reduce(last_key, values)

                last_key = key
                values.clear()

            values.append(value)

    if len(values) > 0:
        reducer.reduce(last_key, values)


def shuffle_parallel(file_name):
    Shuffler(file_name).sort()


def reduce_parallel(x):
    shard_file_name = x[0]
    output_file_name = x[1]
    reduce_file(shard_file_name, SumReducer(emitter=FileEmitter(output_file_name)))


def map_reduce(file_name):
    if os.path.exists('data/shard'):
        shutil.rmtree('data/shard')
    os.mkdir('data/shard')

    if os.path.exists('data/output'):
        shutil.rmtree('data/output')
    os.mkdir('data/output')

    processing_pool = multiprocessing.Pool(processes=2)
    shards_count = 10
    shard_file_names = ['data/shard/{:{fill}4}.txt'.format(x, fill=0) for x in range(shards_count)]
    output_file_names = ['data/output/{:{fill}4}.txt'.format(x, fill=0) for x in range(shards_count)]

    # MAP PHASE
    grand_start = start = time.time()
    map_file(file_name, IdentityMapper(unique_keys=shards_count * 100,
                                       emitter=FilesEmitter(shard_file_names, DefaultShardingFunction(shards_count))))
    end = time.time()
    print('Map phase: {:.3f}s'.format(end - start))

    # SHUFFLE PHASE
    start = time.time()
    processing_pool.map(shuffle_parallel, shard_file_names)
    end = time.time()
    print('Shuffle phase: {:.3f}s'.format(end - start))

    # REDUCE PHASE
    start = time.time()
    processing_pool.map(reduce_parallel, zip(shard_file_names, output_file_names))
    grand_end = end = time.time()
    print('Reduce phase: {:.3f}s'.format(end - start))

    print('Total time: {:.3f}s'.format(grand_end - grand_start))

    # SUMMARIZATION PHASE
    sum, count = summarize_and_divide(output_file_names)
    print('{} / {} = {}'.format(sum, count, sum * 1.0 / count))


def summarize_and_divide(file_names):
    sum = 0
    count = 0
    for file_name in file_names:
        with open(file_name, 'r') as file:
            for line in file:
                key, value = key_value_decode(line)
                sum += value[0]
                count += value[1]

    return sum, count


def main():
    file_name = 'data/input/0000.txt'
    map_reduce(file_name)


if __name__ == '__main__':
    main()
