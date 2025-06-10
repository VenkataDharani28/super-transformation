# Load required libraries
library(dplyr)
library(stringr)
library(lubridate)
library(stringi)

# Source vector_ops.R if not part of a package
# source("vector_ops.R")

preprocess_data <- function(df) {
  # Prepare character fields
  char_cols <- sapply(df, is.character)
  df[char_cols] <- lapply(df[char_cols], prepare_character_fields)
  
  # Prepare indicator fields (example: columns ending with '_flag', '_ind', or 'indicator')
  indicator_cols <- grep("_flag$|_ind$|indicator$", names(df), value = TRUE)
  for (col in indicator_cols) {
    df[[col]] <- prepare_indicator_fields(df[[col]])
  }
  
  # Prepare date fields
  date_cols <- sapply(df, function(x) inherits(x, c("POSIXct", "POSIXlt", "Date")))
  df[date_cols] <- lapply(df[date_cols], prepare_date_fields)
  
  # Prepare numeric fields
  num_cols <- sapply(df, is.numeric)
  df[num_cols] <- lapply(df[num_cols], prepare_numeric_fields)
  
  # Clean text columns (could be character columns)
  for (col in names(df)[char_cols]) {
    df[[col]] <- clean_text(df[[col]])
  }
  
  # Example custom features (uncomment and adjust as needed)
  df$age_of_home_at_loss <- add_age_of_home_at_loss(df$LOSS_DT, df$YEAR_BUILT)
  df$age_of_roof_at_loss <- add_age_of_roof_at_loss(df$LOSS_DT, df$ROOF_YEAR)
  df$deductible_num <- convert_ded_txt_to_number(df$DEDUCTIBLE, df$DWELLING_LIMIT)
  
  return(df)
}
