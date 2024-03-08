# Let's generate a CSV file with 1 million rows and 5 columns using Python

# Import necessary libraries
import pandas as pd
import numpy as np
import sys

if len(sys.argv) != 3:
    print("Please pass the inputs: python3 create_data.py <row_count> <filename>")
    sys.exit()

# Define the number of rows and columns
num_rows = int(sys.argv[1])
num_cols = 5

# Generate sample data
data = np.random.rand(num_rows, num_cols)
df = pd.DataFrame(data, columns=[f'Column_{i+1}' for i in range(num_cols)])

# Specify the file path (adjust this path as needed)
file_path = sys.argv[2]

# Write the DataFrame to a CSV file
df.to_csv(file_path, index=False)