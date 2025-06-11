# Load required libraries
library(dplyr)
library(stringr)
library(lubridate)
library(stringi)
library(nanoparquet)
library(glue)

# ---------------------------
# Preprocessing Functions
# ---------------------------

prepare_character_fields <- function(x) {
  if (!is.character(x)) {
    x <- as.character(x)
  }
  x <- if_else(str_detect(x, "^\\s*$") | x == "-", "blnkVal", x)
  x <- trimws(x)
  x
}

prepare_indicator_fields <- function(x) {
  case_when(
    x %in% c(TRUE, 1L, "1", "Y") ~ 1L,
    x %in% c(FALSE, 0L, "0", "N") ~ 0L,
    x %in% c("U", " ") ~ NA_integer_,
    is.na(x) ~ NA_integer_,
    TRUE ~ 999L
  )
}

prepare_date_fields <- function(x) {
  if (inherits(x, "POSIXct") || inherits(x, "POSIXlt")) {
    x <- as.Date(x)
  }
  x
}

prepare_numeric_fields <- function(x) {
  if (!is.numeric(x)) {
    x <- as.numeric(x)
  }
  x
}

add_age_of_home_at_loss <- function(x, y) {
  age_of_home_at_loss <- year(x) - y
  if_else(y == 0L, NA_real_, age_of_home_at_loss)
}

add_age_of_roof_at_loss <- function(x, y) {
  age_of_roof_at_loss <- year(x) - year(y)
  if_else(y == 0L, NA_real_, age_of_roof_at_loss)
}

convert_ded_txt_to_number <- function(x, y) {
  x <- as.numeric(x)
  if_else(x <= 1L, x * y, x)
}

clean_text <- function(x) {
  x <- x %>%
    stri_replace_na("_MISSING_") %>%
    replace(. %in% c("NaN", "NULL", "None", "nan", "Null", "", "-"), "_MISSING_") %>%
    tolower()
  x <- gsub("[[:digit:]]", "", x)
  x <- gsub("[[:punct:]]", "", x)
  x <- gsub("[_]+", " ", x)
  x <- gsub("\n", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

# ---------------------------
# Combined Preprocessing Function
# ---------------------------

preprocess_super_subro <- function(df) {
  df <- df %>%
    mutate(
      across(where(is.character), prepare_character_fields),
      across(where(is.logical), prepare_indicator_fields),
      across(where(is.numeric), prepare_numeric_fields),
      LOSS_DT = prepare_date_fields(LOSS_DT),
      YEAR_BUILT = as.integer(YEAR_BUILT),
      ROOF_INSTALLED_YEAR = as.integer(ROOF_INSTALLED_YEAR),
      AGE_OF_HOME_AT_LOSS = add_age_of_home_at_loss(LOSS_DT, YEAR_BUILT),
      AGE_OF_ROOF_AT_LOSS = add_age_of_roof_at_loss(LOSS_DT, ROOF_INSTALLED_YEAR),
      DEDUCTIBLE = convert_ded_txt_to_number(DEDUCTIBLE, DWELLING_LIMIT),
      across(contains("DESC"), clean_text)
    )
  return(df)
}

# ---------------------------
# Data I/O Helpers
# ---------------------------

write_data_to_parquet <- function(df, output_fp, dim = NULL) {
  stopifnot(
    is.data.frame(df),
    is.null(dim) || all(dim(df) == dim),
    !file.exists(output_fp)
  )
  nanoparquet::write_parquet(df, output_fp)
}

read_data_from_parquet <- function(data_fp) {
  stopifnot(is.character(data_fp), file.exists(data_fp))
  nanoparquet::read_parquet(data_fp)
}

split_parquet_data <- function(data_fp, output_dir, split_col = "SCORING_MODEL_NAME") {
  stopifnot(dir.exists(output_dir), is.character(split_col))

  dat_to_split <- read_data_from_parquet(data_fp)

  stopifnot(
    split_col %in% names(dat_to_split),
    length(unique(dat_to_split[[split_col]])) < 20L
  )

  dfs_to_write <- split(dat_to_split, dat_to_split[[split_col]])
  fp_builder <- function(x) file.path(output_dir, paste0(x, ".parquet"))

  lapply(names(dfs_to_write), function(name) {
    write_data_to_parquet(dfs_to_write[[name]], output_fp = fp_builder(name))
  })

  invisible(dfs_to_write)
}

# ---------------------------
# Combined Entry Point Function
# ---------------------------

process_and_split_super_subro_data <- function(input_fp,
                                               output_dir = NULL,
                                               split_col = "SCORING_MODEL_NAME",
                                               split = TRUE) {
  raw_df <- read_data_from_parquet(input_fp)
  prepped_df <- preprocess_super_subro(raw_df)

  if (isTRUE(split)) {
    stopifnot(!is.null(output_dir))
    temp_fp <- file.path(output_dir, "super_subro_prepped.parquet")
    write_data_to_parquet(prepped_df, temp_fp)
    split_parquet_data(temp_fp, output_dir, split_col)
    return(invisible(prepped_df))
  }

  return(prepped_df)
}


##########################################################################################################################

process_and_split_super_subro_data(
  input_fp = "data/super_subro_raw.parquet",
  output_dir = "data/preprocessed_split",
  split_col = "SCORING_MODEL_NAME",
  split = TRUE
)

##################### final #########################################################################################

#' Prepare character fields for modeling
#'
#' Replaces blanks and dashes with 'blnkVal' and trims whitespace.
#'
#' @param x A character vector
#'
#' @return A cleaned character vector
prepare_character_fields <- function(x) {
  if (!is.character(x)) {
    x <- as.character(x)
  }
  x <- if_else(str_detect(x, "^\\s*$") | x == "-", "blnkVal", x)
  x <- trimws(x)
  x
}

#' Prepare binary indicator fields for modeling
#'
#' Converts multiple TRUE/FALSE representations to 1/0 or NA
#'
#' @param x A vector of logical or categorical indicator values
#'
#' @return An integer vector of 1, 0, or NA
prepare_indicator_fields <- function(x) {
  case_when(
    x %in% c(TRUE, 1L, "1", "Y") ~ 1L,
    x %in% c(FALSE, 0L, "0", "N") ~ 0L,
    x %in% c("U", " ") ~ NA_integer_,
    is.na(x) ~ NA_integer_,
    TRUE ~ 999L
  )
}

#' Convert date/time field to Date
#'
#' Converts POSIXct or POSIXlt to Date if needed
#'
#' @param x A date/time vector
#'
#' @return A Date vector
prepare_date_fields <- function(x) {
  if (inherits(x, "POSIXct") || inherits(x, "POSIXlt")) {
    x <- as.Date(x)
  }
  x
}

#' Prepare numeric fields for modeling
#'
#' Converts non-numeric values to numeric
#'
#' @param x A numeric or character vector
#'
#' @return A numeric vector
prepare_numeric_fields <- function(x) {
  if (!is.numeric(x)) {
    x <- as.numeric(x)
  }
  x
}

#' Calculate home age at time of loss
#'
#' @param x Date of loss (Date)
#' @param y Year built (integer)
#'
#' @return Age of home at loss (numeric)
add_age_of_home_at_loss <- function(x, y) {
  age_of_home_at_loss <- year(x) - y
  if_else(y == 0L, NA_real_, age_of_home_at_loss)
}

#' Calculate roof age at time of loss
#'
#' @param x Date of loss (Date)
#' @param y Roof installation year (integer or Date)
#'
#' @return Age of roof at loss (numeric)
add_age_of_roof_at_loss <- function(x, y) {
  age_of_roof_at_loss <- year(x) - year(y)
  if_else(y == 0L, NA_real_, age_of_roof_at_loss)
}

#' Convert deductible text field to numeric amount
#'
#' Applies percentage logic when value <= 1
#'
#' @param x Deductible amount (character or numeric)
#' @param y Dwelling limit (numeric)
#'
#' @return Deductible value as numeric
convert_ded_txt_to_number <- function(x, y) {
  x <- as.numeric(x)
  if_else(x <= 1L, x * y, x)
}

#' Clean free-text fields
#'
#' Converts NA/blank/nulls to _MISSING_, removes punctuation/numbers, trims
#'
#' @param x A character vector
#'
#' @return A cleaned, lowercased character vector
clean_text <- function(x) {
  x <- x %>%
    stri_replace_na("_MISSING_") %>%
    replace(. %in% c("NaN", "NULL", "None", "nan", "Null", "", "-"), "_MISSING_") %>%
    tolower()
  x <- gsub("[[:digit:]]", "", x)
  x <- gsub("[[:punct:]]", "", x)
  x <- gsub("[_]+", " ", x)
  x <- gsub("\n", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

#' Apply all super_subro preprocessing steps
#'
#' Applies field-specific cleaners and transformations across the dataset
#'
#' @param df A data.frame with raw super_subro input data
#'
#' @return A cleaned and transformed data.frame
#' @export
preprocess_super_subro <- function(df) {
  df <- df %>%
    mutate(
      across(where(is.character), prepare_character_fields),
      across(where(is.logical), prepare_indicator_fields),
      across(where(is.numeric), prepare_numeric_fields),
      LOSS_DT = prepare_date_fields(LOSS_DT),
      YEAR_BUILT = as.integer(YEAR_BUILT),
      ROOF_INSTALLED_YEAR = as.integer(ROOF_INSTALLED_YEAR),
      AGE_OF_HOME_AT_LOSS = add_age_of_home_at_loss(LOSS_DT, YEAR_BUILT),
      AGE_OF_ROOF_AT_LOSS = add_age_of_roof_at_loss(LOSS_DT, ROOF_INSTALLED_YEAR),
      DEDUCTIBLE = convert_ded_txt_to_number(DEDUCTIBLE, DWELLING_LIMIT),
      across(contains("DESC"), clean_text)
    )
  return(df)
}

#' Write data to parquet format
#'
#' @param df A data.frame to write
#' @param output_fp File path for output
#' @param dim Optional dimension check
#'
#' @return NULL
#' @export
write_data_to_parquet <- function(df, output_fp, dim = NULL) {
  stopifnot(
    is.data.frame(df),
    is.null(dim) || all(dim(df) == dim),
    !file.exists(output_fp)
  )
  nanoparquet::write_parquet(df, output_fp)
}

#' Read a parquet file as data.frame
#'
#' @param data_fp Path to parquet file
#'
#' @return A data.frame
#' @export
read_data_from_parquet <- function(data_fp) {
  stopifnot(is.character(data_fp), file.exists(data_fp))
  nanoparquet::read_parquet(data_fp)
}

#' Split parquet data into multiple files by column value
#'
#' @param data_fp Input parquet file path
#' @param output_dir Directory to write split files
#' @param split_col Column to split on (must have <20 unique values)
#'
#' @return Invisibly returns list of data.frames split
#' @export
split_parquet_data <- function(data_fp, output_dir, split_col = "SCORING_MODEL_NAME") {
  stopifnot(dir.exists(output_dir), is.character(split_col))
  dat_to_split <- read_data_from_parquet(data_fp)
  stopifnot(
    split_col %in% names(dat_to_split),
    length(unique(dat_to_split[[split_col]])) < 20L
  )
  dfs_to_write <- split(dat_to_split, dat_to_split[[split_col]])
  fp_builder <- function(x) file.path(output_dir, paste0(x, ".parquet"))
  lapply(names(dfs_to_write), function(name) {
    write_data_to_parquet(dfs_to_write[[name]], output_fp = fp_builder(name))
  })
  invisible(dfs_to_write)
}

#' Full processing and optional splitting of Super Subro data
#'
#' Reads, processes, and optionally splits the data by a key column
#'
#' @param input_fp File path to raw input parquet
#' @param output_dir Directory to write outputs (if split = TRUE)
#' @param split_col Column to split on (default = 'SCORING_MODEL_NAME')
#' @param split Logical, whether to split and write outputs
#'
#' @return Processed data.frame (invisibly if split = TRUE)
#' @export
process_and_split_super_subro_data <- function(input_fp,
                                               output_dir = NULL,
                                               split_col = "SCORING_MODEL_NAME",
                                               split = TRUE) {
  raw_df <- read_data_from_parquet(input_fp)
  prepped_df <- preprocess_super_subro(raw_df)

  if (isTRUE(split)) {
    stopifnot(!is.null(output_dir))
    temp_fp <- file.path(output_dir, "super_subro_prepped.parquet")
    write_data_to_parquet(prepped_df, temp_fp)
    split_parquet_data(temp_fp, output_dir, split_col)
    return(invisible(prepped_df))
  }

  return(prepped_df)
}

######################################################################################################
