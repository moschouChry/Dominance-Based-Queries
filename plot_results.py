import matplotlib.pyplot as plt
import numpy as np


def create_subplot_layout(dimensions):
    """
    Calculate the subplot layout based on the number of dimensions.
    """

    # Find the total number of plots for the subplot layout
    num_plots = (dimensions * dimensions - dimensions) / 2

    columns = (dimensions + 1) // 2 * 2 - 1
    rows = num_plots/columns

    return int(rows), int(columns)


def plot_pairs(data, distribution):

    dimensions = data.shape[1]
    rows, cols = create_subplot_layout(dimensions)

    # Plot scatter plots for all pairs of dimensions
    fig, axes = plt.subplots(rows, cols, figsize=(12, 12))
    fig.suptitle(f"Scatter Plots for all pairs of dimensions for {distribution} distribution")

    counter = 0
    for i in range(dimensions):
        for j in range(i + 1, dimensions):
            row_counter, col_counter = divmod(counter, cols)

            axes[row_counter, col_counter].scatter(data[:, i], data[:, j], alpha=0.5)
            axes[row_counter, col_counter].set_xlabel(f'Dimension {i + 1}')
            axes[row_counter, col_counter].set_ylabel(f'Dimension {j + 1}')

            # Add grid to each subplot
            axes[row_counter, col_counter].grid(True, linestyle='--', alpha=0.7)

            counter += 1

    # Adjust layout to prevent clipping of title
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.show()
