# Features and Predictions####
#' Generate features from new data. It is unclear whether this is actually
#' necessary if we have raw data plus model objects saved. I don't think it is.
#'
#' @param workflow a fitted workflow object
#' @param new_data a data.frame object that contains the necessary columns
#' relevant to `workflow`
#'
#' @return a data.frame object with the features generated from the fitted
#' `recipe` object inside of the `workflow`
#'
#' @importFrom hardhat extract_recipe
#' @importFrom glue glue
#' @importFrom recipes bake
#'
#' @export
generate_features <-  function(workflow, new_data) {
  pkg_tic()
  # TODO: Address PCML metadata columns that need to pass through...

  # Check input
  stopifnot(
    "workflow should be a workflow object" =
      workflows::is_trained_workflow(workflow),
    "new_data should be a data.frame object" = is.data.frame(new_data)
  )
pkg_messenger("Extracting recipe from workflow...")
  wf_recipe <- hardhat::extract_recipe(workflow)

  rc <- dim(new_data)
  msg <- glue::glue("Generating features for new_data ({rc[1]}x{rc[2]})...")
  pkg_messenger(msg)
  features_to_return <- recipes::bake(wf_recipe, new_data = new_data)

  rc <- dim(features_to_return)
  msg <- glue::glue("Returning features ({rc[1]}x{rc[2]}) for new_data...")
  pkg_messenger(msg)

  pkg_toc()

  features_to_return
}
#' Generate predictions from new data. A character vector identifies the
#' metadata columns to retain in addition to the prediction that are created
#' from the fitted `workflow` object.
#'
#' The function will throw an error if the metadata columns are not available.
#'
#' @param workflow a fitted workflow object
#' @param new_data a data.frame object that contains the necessary columns
#' relevant to `workflow`
#'
#' @inheritParams predict_new_data_with_workflow
#'
#' @return a data.frame object with the predictions generated from the fitted
#' `workflow` object and the metadata_cols.
#'
#' @importFrom hardhat extract_recipe
#' @importFrom glue glue
#' @importFrom dplyr select
#' @importFrom tidyselect one_of starts_with
#' @importFrom generics augment
#' @importFrom dplyr percent_rank
#' @import workflows
#'
#' @export
generate_predictions <-  function(workflow, new_data, metadata_cols = NULL) {
  pkg_tic()

  # Check input
  stopifnot(
    "workflow should be a workflow object" =
      workflows::is_trained_workflow(workflow),
    "new_data should be a data.frame object" = is.data.frame(new_data)
  )
rc <- dim(new_data)
  msg <- glue::glue("Generating predictions for new_data ({rc[1]}x{rc[2]})...")
  pkg_messenger(msg)
  predictions_and_raw <- generics::augment(workflow, new_data)
  length_to_keep <- length(metadata_cols)
  # If NULL, this will just be 0
  msg <- glue::glue("Attempting to retain {length_to_keep} metadata_cols...")
  pkg_messenger(msg)

  rc <- dim(predictions_and_raw)
  msg <- glue::glue("Dropping extra columns from result ({rc[1]}x{rc[2]})")
  pkg_messenger(msg)

  predictions_and_cols_to_return <- dplyr::select(
    predictions_and_raw,
    tidyselect::all_of(metadata_cols),
    tidyselect::starts_with(".pred")
    )

  rc <- dim(predictions_and_cols_to_return)
  msg <- glue::glue("Returning result ({rc[1]}x{rc[2]}) for new_data...")
  pkg_messenger(msg)

  pkg_toc()
  return(predictions_and_cols_to_return)
}
