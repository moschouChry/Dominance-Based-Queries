import csv
import numpy as np
import os
import sys
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt
import math

def correlated(dimensions, samples):
    correlation_strength = 0.8
    # Generate a covariance matrix with correlation_strength
    cov_matrix = np.ones((dimensions, dimensions)) * correlation_strength
    np.fill_diagonal(cov_matrix, 1.0)  # Set diagonal elements to 1

    # Generate correlated data using the covariance matrix
    data = np.random.multivariate_normal(mean=np.zeros(dimensions), cov=cov_matrix, size=samples)

    return data

def anticorrelated(dimensions, samples):

    anticorrelation_strength = 0.8

    # Generate a covariance matrix with anticorrelation_strength
    cov_matrix = np.ones((dimensions, dimensions)) * anticorrelation_strength
    np.fill_diagonal(cov_matrix, 1.0)  # Set diagonal elements to 1

    # Generate correlated data using the covariance matrix
    data = np.random.multivariate_normal(mean=np.zeros(dimensions), cov=cov_matrix, size=samples)

    # Invert even dimensions to make them anticorrelated
    for d in range(dimensions):
        if d%2 == 0:
            data[:, d] = data[:, d].max() - data[:, d]

    return data
def normal(dimensions, samples):

    data = np.random.randn(samples, dimensions)
    return data

def uniform(dimensions, samples):

    data = np.random.uniform(size=(samples, dimensions))
    return data


def plot_function(data, distribution):
    dimensions = data.shape[1]

    # Plot scatter plots for all pairs of dimensions
    fig, axes = plt.subplots(dimensions, dimensions, figsize=(12, 12))
    fig.suptitle(f"Scatter Plots for All Pairs for {distribution} distribution")

    for i in range(dimensions):
        for j in range(dimensions):
            if i == j:
                axes[i, j].hist(data[:, i], bins=20, color='skyblue', alpha=0.7)
                axes[i, j].set_title(f'Dimension {i + 1}')
            else:
                axes[i, j].scatter(data[:, i], data[:, j], alpha=0.5)
                axes[i, j].set_xlabel(f'Dimension {i + 1}')
                axes[i, j].set_ylabel(f'Dimension {j + 1}')

    plt.tight_layout(rect=[0, 0, 1, 0.96])  # Adjust layout to prevent clipping of title
    plt.show()

def generate_data(distribution, dimensions, samples, plot_sample):
    if distribution == "correlated":
        data = correlated(dimensions, samples)
    elif distribution == "anticorrelated":
        data = anticorrelated(dimensions, samples)
    elif distribution == "normal":
        data = normal(dimensions, samples)
    elif distribution == "uniform":
        data = uniform(dimensions, samples)
    else:
        print("Invalid distribution. Choose from: correlated, anticorrelated, normal, uniform.")
        sys.exit(1)

    # Normalize Data
    scaler = MinMaxScaler()
    normalized_data = scaler.fit_transform(data)

    # Create 'data' folder if it doesn't exist
    os.makedirs("data", exist_ok=True)

    # Save the generated data to a CSV file
    filename = f"data/{distribution}_data_{dimensions}D_{samples}S.csv"
    with open(filename, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerows(normalized_data)

    print(f"Dataset with {distribution} distribution, {dimensions} dimensions and"
          f" {samples} samples was successfully created!")

    # Plot a sample of the dataset if plot_pairs is True
    if plot_pairs:
        plot_function(normalized_data, distribution)


if __name__ == "__main__":
    if len(sys.argv) not in (4, 5):
        print("Usage: python data_generator.py <distribution> <dimensions> <samples> [--plot_pairs]")
        sys.exit(1)

    distribution = sys.argv[1]
    dimensions = int(sys.argv[2])
    samples = int(sys.argv[3])

    plot_pairs = False
    if len(sys.argv) == 5 and sys.argv[4] == "--plot_pairs":
        plot_pairs = True

    generate_data(distribution, dimensions, samples, plot_pairs)
