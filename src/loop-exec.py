import os
import sys
import glob
import yaml
import shutil
import random
import math

os.system('which python3')
base_dir = cfg_file = sys.argv[1]

files = glob.glob(base_dir + '/**.yaml')

for i in range(0, 30):
    for filepath in files:
        dest = filepath.replace('.yaml', '-tmp.yaml')
        shutil.copy(filepath, dest)
        filepath = dest
        if 'epsilon-rnd' in filepath or 'top-mix' in filepath:
            config = None
            with open(filepath, 'r') as stream:
                config = yaml.load(stream, Loader=yaml.FullLoader)
            with open(filepath, 'w') as stream:
                config['agent-config']['epsilon-random-seed'] = int(math.floor(random.random() * 1000))
                yaml.dump(config, stream, Dumper=yaml.CDumper)
        if 'query-gen-rnd' in filepath or 'top-mix' in filepath:
            config = None
            with open(filepath, 'r') as stream:
                config = yaml.load(stream, Loader=yaml.FullLoader)
            with open(filepath, 'w') as stream:
                config['query-generator-config']['random-seed'] = int(math.floor(random.random() * 1000))
                yaml.dump(config, stream, Dumper=yaml.CDumper)
        print('starting for ' + filepath)
        os.system('python3 main.py ' + filepath)
        print('ended for ' + filepath)
        os.remove(filepath)
        print('removed ' + filepath)
