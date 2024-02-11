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

# rcParams['font.sans-serif'] = ['Roboto']
# rcParams['font.sans-serif'] = [font_path]

# data = {
#     '#dimensions': [2, 4, 6, 8, 2, 4, 6, 8, 2, 4, 6, 8, 2, 4, 6, 8, 2, 4, 6, 8, 2, 4, 6, 8, 2, 4, 6, 8, 2, 4, 6, 8, 2, 4, 6, 8, 2, 4, 6, 8, 2, 4, 6, 8, 2, 4, 6, 8],
#     'time': [43.1569141976, 87.2436337603, 136.3552903471, 249.7001434535, 72.980796634, 126.03652911399999, 194.2947521758, 328.7226466711, 136.401574561, 220.77590736390002, 307.16600837939995, 470.2509314174, 29.9724553911, 59.268771308400005, 98.54490290020001, 158.3072674722, 47.2466281546, 84.7841272922, 126.99840969289998, 220.16050118910007, 92.2079275554, 140.6786645482, 200.3485135933, 298.26493099209995, 20.711314176600006, 43.5895383192, 76.86931515620002, 98.85385401469999, 32.5014328856, 57.0361550974, 98.8895542566, 137.86122372440002, 67.66734111119999, 103.0187327432, 165.6022197702, 208.9157736016, 15.3365346967, 30.2594247405, 63.1306640248, 72.6787041758, 28.1027771379, 46.993643207000005, 85.58527151630001, 114.3031596082, 58.21929403200001, 82.82101134399998, 130.4543293625, 159.0869082352],
#     'color': ['red', 'red', 'red', 'red', 'green', 'green', 'green', 'green', 'blue', 'blue', 'blue', 'blue', 'red', 'red', 'red', 'red', 'green', 'green', 'green', 'green', 'blue', 'blue', 'blue', 'blue', 'red', 'red', 'red', 'red', 'green', 'green', 'green', 'green', 'blue', 'blue', 'blue', 'blue', 'red', 'red', 'red', 'red', 'green', 'green', 'green', 'green', 'blue', 'blue', 'blue', 'blue'],
#     'marker': ['dotted', 'dotted', 'dotted', 'dotted', 'dotted', 'dotted', 'dotted', 'dotted', 'dotted', 'dotted', 'dotted', 'dotted', 'dashdot', 'dashdot', 'dashdot', 'dashdot', 'dashdot', 'dashdot', 'dashdot', 'dashdot', 'dashdot', 'dashdot', 'dashdot', 'dashdot', 'dashed', 'dashed', 'dashed', 'dashed', 'dashed', 'dashed', 'dashed', 'dashed', 'dashed', 'dashed', 'dashed', 'dashed', 'solid', 'solid', 'solid', 'solid', 'solid', 'solid', 'solid', 'solid', 'solid', 'solid', 'solid', 'solid']
# }

# combinations = list(itertools.product(data['#dimensions'], data['color'], data['marker']))

# new_data = {
#     '#dimensions': [],
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

df = pd.read_csv("./top_k_dominant/logs/results_dimensions.csv").drop_duplicates().dropna()
# print(df.shape)
df = df[df['distribution'] == sys.argv[1]]
df['marker'] = df['#cores'].map(num_cores_map)
df['color'] = df['K'].map(k_color_map)

marker_color_combinations = list(itertools.product(df['marker'].unique(), df['color'].unique()))

fig, ax = plt.subplots(figsize=(10, 8))

for marker, color in marker_color_combinations:
    subset = df[(df['marker'] == marker) & (df['color'] == color)]
    subset = subset.drop_duplicates(subset='#dimensions')
    # print(subset.shape)σ
    if subset.shape[0] == 0:
        continue
    ax.plot(subset['#dimensions'], subset['time'], linestyle=marker, color=color, label=f'#cores: {reversed_num_cores_map[marker]}, K: {reversed_k_color_map[color]}')

ax.set_xticks([2, 4, 6, 8])
ax.set_xlabel('#dimensions')
ax.set_ylabel('Execution Time (seconds)')
ax.set_title(f'Execution time for top-k dominating \n Dataset with 1 million points and {sys.argv[1]} distribution')

ax.legend()
plt.yscale('log')
plt.grid()

plt.savefig(f'./{sys.argv[1]}_dimensions.png')

grouped_df = df.groupby(by=['#dimensions', 'K']).apply(lambda a: a[:]).drop(columns=['distribution', '#samples', '#dimensions', 'marker', 'color', 'K'])

grouped_df.to_html(f'./{sys.argv[1]}_dimensions.html')
