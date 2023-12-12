import csv
import os
import sys
from sklearn.preprocessing import MinMaxScaler

from distributions import correlated, anticorrelated, normal, uniform
from plot_results import plot_pairs


def generate_data(distribution, dimensions, samples, plot):
    """
    Generate synthetic data based on the specified distribution and save it to a CSV file.
    The format of the name of the CSV file is "{distribution}_data_{dimensions}D_{samples}S".
    Example for 1000, normal, 4-dimensional data : "normal_data_4D_1000S"

    Args:
        distribution (str): The type of data distribution to generate. Choose from:
                            correlated, anticorrelated, normal, uniform.
        dimensions (int): The number of dimensions for the generated data.
        samples (int): The number of data samples to generate.
        plot (bool): If True, a plot of the distribution between each pair of dimensions will be created.

    Returns:
        None
    """
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

    # Plot the distribution between each pair of dimensions if plot is True
    if plot:
        plot_pairs(normalized_data, distribution)


if __name__ == "__main__":
    if len(sys.argv) not in (4, 5):
        print("Usage: python data_generator.py <distribution> <dimensions> <samples> [--plot]")
        sys.exit(1)

    distribution = sys.argv[1]
    dimensions = int(sys.argv[2])
    samples = int(sys.argv[3])

    plot = False
    if len(sys.argv) == 5:
        if sys.argv[4] == "--plot":
            plot = True
        else:
            print("Usage: python data_generator.py <distribution> <dimensions> <samples> [--plot]")
            sys.exit(1)

    generate_data(distribution, dimensions, samples, plot)
