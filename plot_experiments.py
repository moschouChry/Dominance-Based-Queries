import pandas as pd
import matplotlib.pyplot as plt
import itertools
import numpy as np

data = {
    '#dimensions': [2, 4, 6, 8],
    'time': [10, 20, 15, 25],
    'color': ['red', 'blue', 'green', 'red'],
    'marker': ['dashed', 'dashdot', 'dotted', 'solid']
}

combinations = list(itertools.product(data['#dimensions'], data['color'], data['marker']))

new_data = {
    '#dimensions': [],
    'color': [],
    'marker': [],
    'time': []
}

num_cores_map = {
    'dashed': 2,
    'dashdot': 4,
    'dotted': 8,
    'solid': 16
}

k_color_map = {
    'red': 10,
    'green': 25,
    'blue': 50
}

for combo in combinations:
    new_data['#dimensions'].append(combo[0])
    new_data['color'].append(combo[1])
    new_data['marker'].append(combo[2])

    new_data['time'].append(np.random.randint(combo[0] + k_color_map[combo[1]] + num_cores_map[combo[2]], 1 + combo[0] + k_color_map[combo[1]] + num_cores_map[combo[2]]))

df = pd.DataFrame(new_data).drop_duplicates()
print(df.shape)

marker_color_combinations = list(itertools.product(df['marker'].unique(), df['color'].unique()))

fig, ax = plt.subplots(figsize=(20, 10))

for marker, color in marker_color_combinations:
    subset = df[(df['marker'] == marker) & (df['color'] == color)]
    subset = subset.drop_duplicates(subset='#dimensions')
    print(subset.shape)

    ax.plot(subset['#dimensions'], subset['time'], linestyle=marker, color=color, label=f'#cores: {num_cores_map[marker]}, K: {k_color_map[color]}')

ax.set_xticks([2, 4, 6, 8])
ax.set_xlabel('#dimensions')
ax.set_ylabel('Time')
ax.set_title('Results')

ax.legend()
plt.show()
