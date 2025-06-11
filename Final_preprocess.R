######shared_utils#############
#' Prepare character fields (General)
#'
#' @param x takes an argument that will convert non-character
#' #fields to character
#' @importFrom stringr str_detect
#' @return character
#' @export
prepare_character_fields <- function(x) {
  # Telemetry measures
  tmtry <- list()
  # Convert to character if not already
  if (is.character(x)) {
    tmtry$converts <- 0L   # Indicates no conversion needed
  } else {
    x <- as.character(x)
    tmtry$converts <- 1L  # Indicates conversion happened
  }
  # Replace all whitespace or empty strings or "-" with "blnkVal"
  # Detect and count empty strings
  blanks <- sum(stringr::str_detect(x, "^\\s*$") | x == "-", na.rm = TRUE)
  tmtry$blanks <- blanks
  x <- if_else(stringr::str_detect(x, "^\\s*$") | x == "-", "blnkVal", x)
  # Trim leading or trailing whitespace
  # Detect and count whitespace
  ws <- sum(stringr::str_detect(x, "^\\s+|\\s+$"), na.rm = TRUE)
  tmtry$ws <- ws
  x <- trimws(x)
  x
}
#' Prepare indicator fields (General)
#'
#' @param x takes an argument that will convert non-indicator
#' fields to indicators
#' @importFrom dplyr %>%
#' @return indicator
#' @export
prepare_indicator_fields <- function(x) {
  # Convert to numeric indicators
  x <- dplyr::case_when(
    x %in% c(TRUE, 1L, "1", "Y") ~ 1L,
    x %in% c(FALSE, 0L, "0", "N") ~ 0L,
    x %in% c("U", " ") ~ NA_integer_,
    is.na(x) ~ NA_integer_,
    TRUE ~ 999L
  )
  x
}
#' Prepare date fields (General)
#'
#' @param x takes an argument that will create a date format
#'
#' @return date
#' @export
prepare_date_fields <- function(x) {
  # Check for POSIXct or POSIXlt and convert to Date
  if (inherits(x, "POSIXct") || inherits(x, "POSIXlt")) {
    x <- as.Date(x)
  }
  x
}
#' Prepare numeric fields (General)
#'
#' @param x takes an argument that will convert non-numerical fields to numeric
#'
#' @return numeric fields
#' @export
prepare_numeric_fields <- function(x) {
  # Convert to numeric
  if (!is.numeric(x)) {
    x <- as.numeric(x)
  }
  x
}
#' Estimating the age of the house at loss (Property Custom function)
#'
#' @param x  X can take any date, such as LOSS_DT
#' @param y  Y take the year the house is built.
#' @importFrom lubridate year
#' @return a average house age at loss
#' @export
add_age_of_home_at_loss <- function(x, y) {
  # Calculate the age of the home at loss
  age_of_home_at_loss <- lubridate::year(x) - y
  # Set age to NA where the year built is 0
  if (any(y == 0L)) {
    age_of_home_at_loss <- if_else(y == 0L, NA_real_, age_of_home_at_loss)
  }
  age_of_home_at_loss
}
#' Estimating the age of the roof at loss (Property Custom function)
#'
#' @param x  X can take any date, such as LOSS_DT, FNOL_DT
#' @param y  Y take the year the roof is installed.
#' @importFrom lubridate year
#' @importFrom dplyr if_else
#' @return a average house age at loss
#' @export
add_age_of_roof_at_loss <- function(x, y) {
  # Calculate the age of the home at loss
  age_of_roof_at_loss <- lubridate::year(x) - lubridate::year(y)
  # Set age to NA where the year built is 0
  age_of_roof_at_loss <- dplyr::if_else(y == 0L, NA_real_, age_of_roof_at_loss)
  age_of_roof_at_loss
}
#' Convert deductibles to numeric (Property Custom function)
#'
#' @param x Deductible
#' @param y dwelling limit
#' @importFrom dplyr if_else
#' @return An adjusted `DEDUCTIBLE` field
#' @export
convert_ded_txt_to_number <- function(x, y) {
  x <- as.numeric(x)
  to_return <- dplyr::if_else(x <= 1L, x * y, x)
  to_return
}
#' Text preprocessing function (Property Custom Function)
#' This function preprocess text cols replace missing text rows with _MISSING_
#' remove underline, digit, punctuation, space and trimws the texts.
#' @param x A vector of text to preprocess
#' @importFrom  dplyr %>%
#' @return A clean preprocessed version of x
#' @export
clean_text <- function(x) {
  x <- x %>%
    stringi::stri_replace_na("_MISSING_") %>%
    replace(x %in% c("NaN", "NULL", "None", "nan", "Null", "", "-"),
            "_MISSING_") %>%
    tolower()
  x <- gsub("[[:digit:]]", "", x)
  x <- gsub("[[:punct:]]", "", x)
  x <- gsub("[\\_]+", " ", x, fixed = TRUE)
  x <- gsub("\n", " ", x, fixed = TRUE)
  x <- gsub("\\s+", " ", x)
  x <- trimws(x)
  x
}


######prop_preprocess########

library(nanoparquet)
source("shared_utils.R")  # your general + property-specific preprocessing functions

preprocess_prop <- function() {
  df <- read_parquet("input_raw.parquet")

  df$CHAR_FIELD <- prepare_character_fields(df$CHAR_FIELD)
  df$IND_FIELD  <- prepare_indicator_fields(df$IND_FIELD)
  df$DATE_FIELD <- prepare_date_fields(df$DATE_FIELD)
  df$NUM_FIELD  <- prepare_numeric_fields(df$NUM_FIELD)

  df$AGE_HOME_AT_LOSS <- add_age_of_home_at_loss(df$LOSS_DT, df$YEAR_BUILT)
  df$AGE_ROOF_AT_LOSS <- add_age_of_roof_at_loss(df$LOSS_DT, df$ROOF_INSTALLED_DATE)
  df$DEDUCTIBLE       <- convert_ded_txt_to_number(df$DEDUCTIBLE, df$DWELLING_LIMIT)
  df$DESC_FIELD       <- clean_text(df$DESC_FIELD)

  write_parquet(df, "output_preprocessed.parquet")
}




###########################pipeline.R#################################

# pipeline.R

# Load dependencies
source("vectorops.R")
source("io.R")
source("message.R")

library(dplyr)

# --- Reusable Preprocessing Functions ---

# Apply property data preprocessors by field type
apply_property_preprocessors <- function(df) {
  df %>%
    mutate(across(where(is.character), prepare_character_fields)) %>%
    mutate(across(where(is.logical), prepare_indicator_fields)) %>%
    mutate(across(where(lubridate::is.Date), prepare_date_fields)) %>%
    mutate(across(where(is.numeric), prepare_numeric_fields))
}

# Add engineered features (property-specific)
add_property_features <- function(df) {
  df %>%
    mutate(
      AGE_HOME = add_age_of_home_at_loss(LOSS_DT, YEAR_BUILT),
      AGE_ROOF = add_age_of_roof_at_loss(LOSS_DT, ROOF_INSTALL_DT),
      DESCRIPTION_CLEAN = clean_text(DESCRIPTION)
    )
}

# Full property preprocessing pipeline
process_property_data <- function(input_fp, output_fp) {
  pkg_tic()
  df <- read_data_from_parquet(input_fp)
  df <- apply_property_preprocessors(df)
  df <- add_property_features(df)
  write_data_to_parquet(df, output_fp)
  pkg_toc()
}

# Auto data pipeline (split by SCORING_MODEL_NAME)
process_auto_data <- function(input_fp, output_dir) {
  pkg_tic()
  split_parquet_data(input_fp, output_dir, split_col = "SCORING_MODEL_NAME")
  pkg_toc()
}

# Main dispatcher
run_preprocessing <- function(data_type, input_fp, output_path) {
  stopifnot(data_type %in% c("auto", "property"))
  if (data_type == "auto") {
    process_auto_data(input_fp, output_path)
  } else if (data_type == "property") {
    process_property_data(input_fp, output_path)
  }
}


##################### preprocess.R #############################
#!/usr/bin/env Rscript
library(optparse)
library(aws.s3)       # for S3 interactions; install.packages("aws.s3")
library(glue)

# Parse arguments
option_list <- list(
  make_option(c("-t", "--type"), type="character", default=NULL,
              help="Data type: 'auto' or 'property'", metavar="type"),
  make_option(c("-b", "--bucket"), type="character", default=NULL,
              help="StorageGRID bucket name", metavar="bucket"),
  make_option(c("-k", "--key"), type="character", default=NULL,
              help="StorageGRID object key (input file path)", metavar="key"),
  make_option(c("-o", "--output_key"), type="character", default=NULL,
              help="Output StorageGRID object key (file path to save)", metavar="output_key")
)
opt_parser <- OptionParser(option_list=option_list)
opt <- parse_args(opt_parser)

if (is.null(opt$type) || is.null(opt$bucket) || is.null(opt$key) || is.null(opt$output_key)) {
  stop("Must specify --type, --bucket, --key, and --output_key")
}

source("pipeline.R")

# Local temp files to work on
input_local_fp <- tempfile(fileext = ".parquet")
output_local_fp <- tempfile(fileext = ".parquet")

# Step 1: Download input parquet from StorageGRID
cat(glue("Downloading s3://{opt$bucket}/{opt$key} to {input_local_fp}\n"))
success <- aws.s3::save_object(
  object = opt$key,
  bucket = opt$bucket,
  file = input_local_fp
)
if (!success) stop("Failed to download input file from StorageGRID")

# Step 2: Run preprocessing pipeline
run_preprocessing(opt$type, input_local_fp, output_local_fp)

# Step 3: Upload processed parquet back to StorageGRID
cat(glue("Uploading processed file to s3://{opt$bucket}/{opt$output_key}\n"))
put_success <- aws.s3::put_object(
  file = output_local_fp,
  object = opt$output_key,
  bucket = opt$bucket
)
if (!put_success) stop("Failed to upload output file to StorageGRID")

cat("Preprocessing complete!\n")

##################################################################################################

a) apply_property_preprocessors
r
Copy
Edit
apply_property_preprocessors <- function(df) {
  df %>%
    mutate(across(where(is.character), prepare_character_fields)) %>%
    mutate(across(where(is.logical), prepare_indicator_fields)) %>%
    mutate(across(where(lubridate::is.Date), prepare_date_fields)) %>%
    mutate(across(where(is.numeric), prepare_numeric_fields))
}
Takes a dataframe df.

Applies transformations on columns grouped by their data type:

For all character columns, apply prepare_character_fields()

For all logical (boolean) columns, apply prepare_indicator_fields()

For all Date columns (lubridate::is.Date), apply prepare_date_fields()

For all numeric columns, apply prepare_numeric_fields()

Uses dplyr::mutate(across(...)) for efficient column-wise transformation.

These preprocessing functions (prepare_character_fields etc.) are assumed to be defined in vectorops.R.

b) add_property_features
r
Copy
Edit
add_property_features <- function(df) {
  df %>%
    mutate(
      AGE_HOME = add_age_of_home_at_loss(LOSS_DT, YEAR_BUILT),
      AGE_ROOF = add_age_of_roof_at_loss(LOSS_DT, ROOF_INSTALL_DT),
      DESCRIPTION_CLEAN = clean_text(DESCRIPTION)
    )
}
Adds engineered features (new columns) relevant for property data:

AGE_HOME: Calculated by comparing loss date (LOSS_DT) and home build year (YEAR_BUILT)

AGE_ROOF: Calculated similarly using roof install date (ROOF_INSTALL_DT)

DESCRIPTION_CLEAN: A cleaned/processed version of the DESCRIPTION text field

These helper functions are also assumed to be from your vectorops.R.

3. Full property preprocessing pipeline
r
Copy
Edit
process_property_data <- function(input_fp, output_fp) {
  pkg_tic()
  df <- read_data_from_parquet(input_fp)
  df <- apply_property_preprocessors(df)
  df <- add_property_features(df)
  write_data_to_parquet(df, output_fp)
  pkg_toc()
}
This function performs the entire property data preprocessing in sequence:

Starts a timer/log with pkg_tic() (from message.R)

Reads raw data from a Parquet file located at input_fp

Applies data type specific preprocessors

Adds engineered property features

Writes the processed data to an output Parquet file at output_fp

Ends the timer/log with pkg_toc()

4. Auto data pipeline
r
Copy
Edit
process_auto_data <- function(input_fp, output_dir) {
  pkg_tic()
  split_parquet_data(input_fp, output_dir, split_col = "SCORING_MODEL_NAME")
  pkg_toc()
}
For auto data (different from property data):

Starts timer/log

Reads the input Parquet file and splits it into multiple files based on unique values of the column "SCORING_MODEL_NAME"

Writes these split Parquet files into the output directory (output_dir)

Ends timer/log

split_parquet_data() is likely from your io.R, handling the split and write logic.

5. Main dispatcher function
r
Copy
Edit
run_preprocessing <- function(data_type, input_fp, output_path) {
  stopifnot(data_type %in% c("auto", "property"))
  if (data_type == "auto") {
    process_auto_data(input_fp, output_path)
  } else if (data_type == "property") {
    process_property_data(input_fp, output_path)
  }
}
This function decides which pipeline to run based on the data_type parameter.

Checks data_type is either "auto" or "property" (error if not).

Calls the relevant full pipeline function:

"auto" calls process_auto_data

"property" calls process_property_data

Summary
pipeline.R is your central processing logic.

It imports helpers and I/O functions.

Defines modular steps to preprocess property and auto data.

Provides a single entry point run_preprocessing() to pick which flow to execute.

Uses dplyr for clean and concise dataframe transformations.

Makes heavy use of custom helper functions in other sourced files.

If you want, I can help you dive deeper into any specific helper function or explain how these work in practice with sample data!







You said:
Checks data_type is either "auto" or "property" (error if not).
you mean prop.parquet and auto.parquet?


ChatGPT said:
Great question! When I say:

Checks data_type is either "auto" or "property" (error if not).

I’m talking about the string value you pass to the run_preprocessing() function to tell it which kind of data pipeline to run — not the file names themselves.

What does this mean practically?
You call the function like this, for example:

r
Copy
Edit
run_preprocessing("property", "prop.parquet", "processed_property.parquet")
or

r
Copy
Edit
run_preprocessing("auto", "auto.parquet", "output_folder/")
The first argument "property" or "auto" is the data_type.

The function uses that to decide whether to run the property pipeline or the auto pipeline.

The second argument is the input file path (e.g., "prop.parquet" or "auto.parquet").

The third argument is the output file or folder path.

So:
"property" and "auto" are labels for your data types (used inside the code).

"prop.parquet" and "auto.parquet" are the actual Parquet file names you provide as input.

If you pass something other than "auto" or "property" as the first argument, the code will raise an error (stop execution), because it doesn’t know which pipeline to run.

############################################################################################################################################################################################

Perfect! Here's how you’d call the function with your actual parquet files:

r
Copy
Edit
# For property data:
run_preprocessing(
  data_type = "property", 
  input_fp = "prop.parquet",       # your property data file
  output_path = "processed_property.parquet"  # where you want to save the processed property data
)

# For auto data:
run_preprocessing(
  data_type = "auto",
  input_fp = "auto.parquet",       # your auto data file
  output_path = "processed_auto_data/"  # a folder where auto split parquet files will be saved
)
Notes:

For property data, the output is a single parquet file.

For auto data, the output is a directory because it splits the data by SCORING_MODEL_NAME and writes multiple parquet files inside that folder.

##################################################################################################################################################



Yes, the code and flow you described do complete the preprocessing job for both property and auto data, assuming:

You have prop.parquet and auto.parquet files as input.

The property pipeline applies the needed field-specific preprocessors, adds engineered features, and writes a processed parquet file.

The auto pipeline splits the input parquet by SCORING_MODEL_NAME and writes multiple parquet files to an output directory.

The helper functions (prepare_character_fields(), add_age_of_home_at_loss(), write_data_to_parquet(), etc.) work as expected and are sourced properly.

You have proper permissions and paths for reading/writing files.

What your run_preprocessing() function does:
Checks the data type is valid (auto or property).

Runs the appropriate processing function:

For property: full preprocessing and feature engineering → single output parquet.

For auto: split parquet based on SCORING_MODEL_NAME → multiple output parquet files in directory.

What it doesn’t do (and you might want to check):
Handle errors gracefully (e.g., missing columns, read/write errors).

Log detailed step-by-step progress except the timing and basic messaging inside pkg_tic()/pkg_toc().

Perform any other validation of input data beyond what your io.R helpers enforce.

Summary
If your goal is to take raw data files (prop.parquet, auto.parquet), preprocess them, and output ready-to-use parquet files (or split parquet files for auto), this design completes that.

##################################################################################################################################################################################################













