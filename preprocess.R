# Load required libraries
library(dplyr)
library(stringr)
library(lubridate)
library(stringi)
library(nanoparquet)
library(glue)

# Source your custom functions if not part of a package
# source("vector_ops.R")
# source("io.R")

# ---- Configuration ----
input_parquet_path <- "path/to/storageGrid/input_data.parquet"   # Update with your actual input path
preprocessed_parquet_path <- "path/to/storageGrid/preprocessed_data.parquet" # Update with your output path
split_output_dir <- "path/to/storageGrid/split_data"              # Update with your output directory
split_col <- "SCORING_MODEL_NAME"                                 # Update with your split column

# ---- Read Data from Storage Grid ----
df <- nanoparquet::read_parquet(input_parquet_path)

# ---- Preprocess Data ----
df_prep <- preprocess_data(df)

# ---- Write Preprocessed Data to Storage Grid ----
nanoparquet::write_parquet(df_prep, file = preprocessed_parquet_path)

# ---- Split the Preprocessed Data and Write Each Split to Storage Grid ----
split_parquet_data(preprocessed_parquet_path, split_output_dir, split_col)
