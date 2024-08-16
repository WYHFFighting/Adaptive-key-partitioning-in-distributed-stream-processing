# -*- coding: utf-8 -*-
import argparse


parser = argparse.ArgumentParser()
# All_Beauty, zipfian_z[1.0, 2.0, 3.0]
parser.add_argument('--data_path', type = str, default = './data/zipfian_z2.0.pickle')
# parser.add_argument('--theta', type = float, default = 0.01)
parser.add_argument('--theta', type = float, default = 0.1)
parser.add_argument('--epsilon', type = float, default = 0.005)
# parser.add_argument('--delta', default = 1.5)
parser.add_argument('--delta', type = float, default = 2.0)
parser.add_argument('--m', type = int, default = 1000)
parser.add_argument('--segment_size', type = int, default = 10000)

args = parser.parse_args()

