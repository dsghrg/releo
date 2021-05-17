import os

os.system('which python3')
for i in range(0, 10):
    print(str(i) + '\tstart executing main.py')
    os.system('python3 main.py')
    print(str(i) + '\tfinished executing main.py')
