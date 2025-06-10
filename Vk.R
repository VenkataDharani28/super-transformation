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
