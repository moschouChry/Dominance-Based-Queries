import csv
import time
from tqdm import tqdm
from plot_results import plot_pairs
import numpy as np
import sys
import os
from data_generator import generate_data


def is_dominated(point, other_point):
    """ Check if 'point' is dominated by 'other_point'. """
    all_temp = all(x >= y for x, y in zip(point, other_point))
    any_temp = any(x > y for x, y in zip(point, other_point))

    return all_temp and any_temp


def bnl_skyline(dataset):
    """ Compute the skyline of the given data using the BNL algorithm. """
    skyline_set = []

    for point in tqdm(dataset, desc="Computing Skyline"):
        # Remove points from skyline that are dominated by the current point
        skyline_set = [sky for sky in skyline_set if not is_dominated(sky, point)]

        # If the point is not dominated by any points in the skyline, add it to the skyline
        if not any(is_dominated(point, sky) for sky in skyline_set):
            skyline_set.append(point)

    return skyline_set


def read_data_from_csv(filepath):
    """ Read data from a CSV file and return it as a list of tuples. """
    dataset = []
    with open(filepath, 'r') as csv_file:
        csvreader = csv.reader(csv_file)
        for row in csvreader:
            # Convert each row to a tuple of floats
            point = tuple(map(float, row))
            dataset.append(point)
    return dataset


if __name__ == "__main__":
    if len(sys.argv) not in (4, 5):
        print("Usage: python baseline_bnl.py <distribution> <dimensions> <samples> [--plot]")
        sys.exit(1)

    plot = False
    if len(sys.argv) == 5:
        if sys.argv[4] == "--plot":
            plot = True
        else:
            print("Usage: python baseline_bnl.py <distribution> <dimensions> <samples> [--plot]")
            sys.exit(1)

    distribution = sys.argv[1]
    dimensions = int(sys.argv[2])
    samples = int(sys.argv[3])

    # Construct the file path
    file_path = f"data/{distribution}_data_{dimensions}D_{samples}S.csv"

    if os.path.exists(file_path):
        print(f"Dataset {file_path} exist.")
    else:
        print(f"Dataset {file_path} does not exist. It will be generated now.")
        generate_data(distribution, dimensions, samples, False)

    # Read and process the data
    data = read_data_from_csv(file_path)
    print(f"Dataset {file_path} loaded.")

    skyline = bnl_skyline(data)

    # Create 'data' folder if it doesn't exist
    os.makedirs("data", exist_ok=True)

    # Save the generated data to a CSV file
    skyline_filename = f"data/{distribution}_data_{dimensions}D_{samples}S_skyline.csv"
    with open(skyline_filename, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerows(skyline)

    print(f"Skyline for dataset {file_path} computed")
    print(f"Points in the skyline: {len(skyline)}")

    data_converted = np.array(data)
    if plot:
        plot_pairs(data_converted, distribution, skyline)

