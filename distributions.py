import numpy as np


def correlated(dimensions, samples):
    # Generate a covariance matrix with correlation_strength
    correlation_strength = 0.8
    cov_matrix = np.ones((dimensions, dimensions)) * correlation_strength
    # Set diagonal elements to 1
    np.fill_diagonal(cov_matrix, 1.0)

    # Generate correlated data using the covariance matrix
    data = np.random.multivariate_normal(mean=np.zeros(dimensions), cov=cov_matrix, size=samples)

    return data


def anticorrelated(dimensions, samples):
    # Generate a covariance matrix with anticorrelation_strength
    anticorrelation_strength = 0.8
    cov_matrix = np.ones((dimensions, dimensions)) * anticorrelation_strength
    # Set diagonal elements to 1
    np.fill_diagonal(cov_matrix, 1.0)

    # Generate correlated data using the covariance matrix
    data = np.random.multivariate_normal(mean=np.zeros(dimensions), cov=cov_matrix, size=samples)

    # Invert even dimensions to make them anticorrelated
    for d in range(dimensions):
        if d % 2 == 0:
            data[:, d] = data[:, d].max() - data[:, d]

    return data


def normal(dimensions, samples):
    data = np.random.randn(samples, dimensions)
    return data


def uniform(dimensions, samples):
    data = np.random.uniform(size=(samples, dimensions))
    return data
