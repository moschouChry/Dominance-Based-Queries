import csv

def calculate_dominance_score(points, k):
    dominance_scores = []
    n = len(points)

    for i in range(n):
        score = 0
        for j in range(n):
            if i != j and all(points[j][d] >= points[i][d] for d in range(len(points[i]))):
                score += 1
        dominance_scores.append((i, score))

    dominance_scores.sort(key=lambda x: x[1], reverse=True)
    return dominance_scores[:k]

def read_csv(file_path):
    points = []
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for i, row in enumerate(reader):
            point = tuple(map(float, row))
            points.append(point)
    return points

file_path = 'dev_data/correlated_data_6D_10000S.csv'
k = 10

points = read_csv(file_path)
top_k_points = calculate_dominance_score(points, k)

print(f"Top {k} points with highest dominance scores:")
for index, score in top_k_points:
    print(f"Point ID: {index}, Dominance Score: {score}")
