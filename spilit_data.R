import pandas as pd

# Read the CSV file
df = pd.read_csv('train.csv')

# Split the data based on unique values in the 'Survived' column
for value in df['Survived'].unique():
    split_df = df[df['Survived'] == value]
    split_df.to_csv(f'survived_{value}.csv', index=False)

##############################################

# Read the CSV file
data <- read.csv("train.csv")

# Split the data by the 'Survived' column
split_data <- split(data, data$Survived)

# Access the split data:
# split_data[["0"]] contains rows where Survived == 0
# split_data[["1"]] contains rows where Survived == 1

# Optionally, write each split to a CSV file
for (val in names(split_data)) {
  write.csv(split_data[[val]], paste0("survived_", val, ".csv"), row.names = FALSE)
}
