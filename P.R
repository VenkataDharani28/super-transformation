predict_new_data_with_workflow <- function(
    workflow_fp, data_fp, output_fp = NULL, metadata_cols = NULL
    ) {
  pkg_tic()
  # If failure, clear the log. It always runs, but is empty if already ran.
  on.exit(suppressMessages(pkg_toc_exit()), after = TRUE)

  ## Load workflow ####
  wf_for_score <- load_workflow(workflow_fp)

  ## Load data ####
  data_to_score <- read_data_from_parquet(data_fp)

  ## Predictions ####
  result <- generate_predictions(wf_for_score, data_to_score, metadata_cols)

  ## Save result ####
  if (!is.null(output_fp)) {
    write_data_to_parquet(result, output_fp)
  }

  ## Return result ####
  pkg_toc_exit()
  return(result)
}
