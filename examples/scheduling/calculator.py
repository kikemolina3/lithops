import pandas as pd

# source = "variant"
# spot_base = 0.3279/float(3600)
# lambda_base = 0.0000166667
# worker_memory = 32/float(20)

source = "terasort"
spot_base = 1.7372/float(3600)
lambda_base = 0.0000166667
worker_memory = 192/float(96)

stats = pd.read_csv(f'{source}/stats.csv')
vms = pd.read_csv(f'{source}/vms.csv')

lambda_price = 0
for i, row in stats.iterrows():
    lambda_price += row['worker_exec_time'] * lambda_base * worker_memory

spot_price = 0
for i, row in vms.iterrows():
    spot_price += (row['end_stamp'] - row['init_stamp']) * spot_base

print(f"Lambda price: {lambda_price}")
print(f"Spot price: {spot_price}")