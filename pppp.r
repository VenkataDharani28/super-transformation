#' Preprocess data for the Super Subro model
#'
#' This function applies all preprocessing steps used by the Super Subro model
#'
#' @param df A data.frame with raw input data
#' @return A processed data.frame ready for modeling
#' @export
preprocess_super_subro <- function(df) {
  # --- Character fields ---
  df <- df %>%
    dplyr::mutate(across(where(is.character), prepare_character_fields))

  # --- Indicator fields ---
  indicator_cols <- c("IS_LITIGATED", "IS_SPECIAL_HANDLING")  # Adjust based on your actual columns
  df <- df %>%
    dplyr::mutate(across(dplyr::any_of(indicator_cols), prepare_indicator_fields))

  # --- Numeric fields ---
  df <- df %>%
    dplyr::mutate(across(where(is.numeric), prepare_numeric_fields))

  # --- Date fields ---
  date_cols <- c("LOSS_DT", "FNOL_DT", "ROOF_INSTALLED_DT")  # Adjust as needed
  df <- df %>%
    dplyr::mutate(across(dplyr::any_of(date_cols), prepare_date_fields))

  # --- Derived fields ---
  if (all(c("LOSS_DT", "YEAR_BUILT") %in% names(df))) {
    df$AGE_HOME_AT_LOSS <- add_age_of_home_at_loss(df$LOSS_DT, df$YEAR_BUILT)
  }

  if (all(c("LOSS_DT", "ROOF_INSTALLED_DT") %in% names(df))) {
    df$AGE_ROOF_AT_LOSS <- add_age_of_roof_at_loss(df$LOSS_DT, df$ROOF_INSTALLED_DT)
  }

  if (all(c("DEDUCTIBLE", "DWELLING_LIMIT") %in% names(df))) {
    df$DEDUCTIBLE_NUM <- convert_ded_txt_to_number(df$DEDUCTIBLE, df$DWELLING_LIMIT)
  }

  # --- Text fields ---
  text_cols <- c("CLAIM_DESCRIPTION", "LOSS_REASON")  # Adjust based on actual dataset
  df <- df %>%
    dplyr::mutate(across(dplyr::any_of(text_cols), clean_text))

  return(df)
}

######################################################################################################

# Load preprocess.R
source("path/to/preprocess.R")

# Use on your raw data
processed_data <- preprocess_super_subro(raw_data)

######################################################################################

