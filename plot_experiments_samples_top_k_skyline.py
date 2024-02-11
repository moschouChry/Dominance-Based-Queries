import pandas as pd
import matplotlib.pyplot as plt
import itertools
import numpy as np
from matplotlib import rcParams
import sys

font_path = "./Roboto-Regular.ttf"
rcParams['font.family'] = 'sans-serif'
rcParams['font.sans-serif'] = ['Roboto']
rcParams['font.serif'] = ['Roboto']
rcParams['font.monospace'] = ['Roboto']
rcParams['font.size'] = 15 

# data = {
#     '#samples': [100000, 1000000, 10000000, 100000, 1000000, 10000000],
#     'time': [3.2589623853, 68.57117275879999, 729.542489378, 2.6557459023, 47.335675441899994, 498.540786487],
#     'color': ['red', 'red', 'red', 'red', 'red', 'red'],
#     'marker': ['dashed', 'dashed', 'dashed', 'solid', 'solid', 'solid']
# }

# combinations = list(itertools.product(data['#dimensions'], data['color'], data['marker']))

# new_data = {
#     '': [],
#     'color': [],
#     'marker': [],
#     'time': []
# }

num_cores_map = {
    2: 'dotted',
    4: 'dashdot',
    8: 'dashed',
    16: 'solid'
}

reversed_num_cores_map = {value: key for key, value in num_cores_map.items()}

k_color_map = {
    10: 'red',
    25: 'green',
    50: 'blue'
}

reversed_k_color_map = {value: key for key, value in k_color_map.items()}


# for combo in combinations:
#     new_data['#dimensions'].append(combo[0])
#     new_data['color'].append(combo[1])
#     new_data['marker'].append(combo[2])

#     new_data['time'].append(combo[2])

df = pd.read_csv("./top_k_dominant/logs/results_samples_skyline.csv").drop_duplicates()
# print(df.shape)
df = df[df['distribution'] == sys.argv[1]]
df['marker'] = df['#cores'].map(num_cores_map)
df['color'] = df['K'].map(k_color_map)

marker_color_combinations = list(itertools.product(df['marker'].unique(), df['color'].unique()))

fig, ax = plt.subplots(figsize=(10, 8))

for marker, color in marker_color_combinations:
    subset = df[(df['marker'] == marker) & (df['color'] == color)]
    subset = subset.drop_duplicates(subset='#samples')
    # print(subset.shape)σ
    if subset.shape[0] == 0:
        continue

    ax.plot(subset['#samples'], subset['time'], linestyle=marker, color=color, label=f'#cores: {reversed_num_cores_map[marker]}, K: {reversed_k_color_map[color]}')

ax.set_xticks([100000, 1000000, 10000000], ['100K', '1M', '10M'])
ax.set_xlabel('#samples')
ax.set_ylabel('Execution Time (seconds)')
ax.set_title(f'Execution time for top-k dominating skyline points \n Dataset with 6 dimensions and {sys.argv[1]} distribution')

ax.legend()
# plt.yscale('log')
plt.grid()

plt.savefig(f'./{sys.argv[1]}_samples_skyline.png')

grouped_df = df.groupby(by=['#samples', 'K']).apply(lambda a: a[:]).drop(columns=['#samples', 'distribution', '#dimensions', 'marker', 'color', 'K'])

grouped_df.to_html(f'./{sys.argv[1]}_samples_skyline.html')
