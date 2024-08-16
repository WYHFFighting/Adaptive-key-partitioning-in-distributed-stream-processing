# -*- coding: utf-8 -*-
import json
import pickle
import numpy as np


def generate_amazon_reviews_dataset(file_path = 'All_Beauty.jsonl', segment_size = 10000):
    # 下载网址
    # https://amazon-reviews-2023.github.io/#load-user-reviews
    tuple_datasets = []
    with open(file_path, 'r') as fp:
        for i, line in enumerate(fp):
            data = json.loads(line.strip())
            key = data['asin']
            data.pop('asin')
            tuple_datasets.append((i % segment_size, key, data))
            # print(i % segment_size, key, data)

    with open(file_path.replace('jsonl', 'pickle'), 'wb') as file:
        pickle.dump(tuple_datasets, file)


def generate_zipfian_dataset(
        num_values = 1000, min_z = 1.0, max_z = 3.0, num_samples = int(1e5), segment_size = int(1e4)
):
    """
    Generate a synthetic dataset based on Zipfian distribution with varying coefficients z.

    Parameters:
    - num_values: int, the number of different integer values in the dataset.
    - min_z: float, minimum value of the Zipfian distribution coefficient, ensured to be greater than 1.
    - max_z: float, maximum value of the Zipfian distribution coefficient.
    - num_samples: int, number of samples to generate for each distribution.
    """
    datasets = {}
    # Ensure all z values are greater than 1 to satisfy the requirements of the zipf distribution
    min_z_adjusted = max(min_z, 1.01)
    # z_values = np.linspace(min_z_adjusted, max_z, num = int((max_z - min_z_adjusted) * 10 + 1))
    z_values = np.linspace(min_z, max_z, 5)
    z_values[0] = min_z_adjusted

    for z in z_values:
        tuple_dataset = []

        # Use numpy's random Zipf generator to create num_samples samples with parameter z
        data = np.random.zipf(a = z, size = num_samples)
        # data = np.mod(data, num_values) + 1
        for i, v in enumerate(data):
            tuple_dataset.append((i % segment_size, v, v))

        with open(f'zipfian_z{z}.pickle', 'wb') as file:
            pickle.dump(tuple_dataset, file)

    return datasets, z_values


def load(path = 'All_Beauty.pickle'):
    with open(path, 'rb') as file:
        data = pickle.load(file)
    temp = data[:10]
    print()


if __name__ == "__main__":
    # generate_zipfian_dataset()
    # generate_amazon_reviews_dataset()
    load('zipfian_z1.0.pickle')
