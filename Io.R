# Data in, data out ####
#' Save data as parquet
#'
#' `write_data_to_parquet` saves a data.frame as a parquet output_fp
#'
#' @inheritParams predict_new_data_with_workflow
#' @param df a data.frame to save
#' @param dim an optional vector of expected dimensions, which will force an
#' error if the dimensions are not as expected
#'
#' @importFrom glue glue
#' @importFrom nanoparquet write_parquet
#'
#' @return NULL
#'
#' @export
write_data_to_parquet <- function(df, output_fp, dim = NULL) {
  pkg_tic()

  # Check input
  stopifnot(
    "df should be a data frame" =  is.data.frame(df),
    "expected dimensions do not match output dimensions" =
      dim(df) == dim,
    "a file already exists at output_fp" = !file.exists(output_fp)
  )
 # For message, rows & columns
  rc <- dim(df)

  # Write out a parquet to output_fp
  pkg_messenger(glue::glue("Saving df ({rc[1]}x{rc[2]}) to {output_fp}..."))

  # Last time we tried arrow in production, it did not work. Using `arrow`
  # is preferred because it is still faster, but `nanoparquet` is a lighter
  # weight package that gets the job done.
  # arrow::write_parquet(x = df, sink = output_fp)
  nanoparquet::write_parquet(x = df, file = output_fp)

  # Report success and elapsed time for this function
  pkg_messenger(glue::glue("Saved df to {output_fp}."))
  pkg_toc()
}
#' Read data from a parquet file
#'
#' `read_data_from_parquet` loads a data.frame from a parquet file
#'
#' @inheritParams predict_new_data_with_workflow
#'
#' @importFrom nanoparquet read_parquet
#' @importFrom glue glue
#'
#' @return a data frame
#'
#' @export
read_data_from_parquet <- function(data_fp) {
  pkg_tic()

  # Check data_fp
  stopifnot("data_fp must be a filepath" = is.character(data_fp),
            "data_fp must exist" = file.exists(data_fp))

  # Read in the file
  pkg_messenger(glue::glue("Reading in {data_fp}..."))
  to_return <- nanoparquet::read_parquet(data_fp)
# Check result # NOT NEEDED...
  # read_parquet only returns a data.frame
  # stopifnot("df should be a data frame" =  is.data.frame(to_return))

  # Report success and elapsed time for this function
  rc <- dim(to_return)
  pkg_messenger(
    glue::glue("Read in {data_fp} as a {rc[1]} x {rc[2]} data frame.")
    )
  pkg_toc()
  to_return
}
#' Split parquet data into parquet data
#'
#' `split_parquet_data` reads in, splits a data.frame into multiple data.frames
#' based on a column, and saves the results as individual parquet files.
#'
#' @inheritParams predict_new_data_with_workflow
#' @param output_dir directory for split data to be written to
#' @param split_col the column that contains the values to split by
#'
#' @importFrom nanoparquet read_parquet
#' @importFrom glue glue
#'
#' @return a data frame
#'
#' @export
split_parquet_data <- function(data_fp, output_dir,
                               split_col = "SCORING_MODEL_NAME") {
  pkg_tic()
  # If failure, clear the log. It always runs, but is empty if already ran.
  on.exit(suppressMessages(pkg_toc_exit()), after = TRUE)

  # Check inputs
  stopifnot("output_dir must exist" = dir.exists(output_dir),
            "split_col must be a string" = is.character(split_col))
# Read in the file
  dat_to_split <- read_data_from_parquet(data_fp)

  # Check result
  # read_parquet only returns a data.frame
  split_col_exists <- split_col %in% names(dat_to_split)
  split_col_wont_explode <- length(unique(dat_to_split[[split_col]])) < 20L
  stopifnot("split_col must be in your data" = split_col_exists,
            "more than 20 levels in split_col" = split_col_wont_explode)

  # Splitting data
  dfs_to_write <- split(dat_to_split, dat_to_split[[split_col]])
  # We could drop one level...
  # dfs[['otherdays']] <- NULL

  # Report success and elapsed time for this function
  rc <- length(names(dfs_to_write))
  pkg_messenger(
    glue::glue("Data from {data_fp} will be split into {rc} files.")
  )
 # Write each file
  fp_builder <- function(x) file.path(output_dir, paste0(x, ".parquet"))
  dont_return <- lapply(names(dfs_to_write),
                        \(x) write_data_to_parquet(
                          dfs_to_write[[x]],
                          output_fp = fp_builder(x))
                        )

  pkg_toc_exit()
  invisible(dont_return)
}

#' Load workflow object
#'
#' `load_workflow` loads the fitted workflow object into the environment
#'
#' @inheritParams predict_new_data_with_workflow
#'
#' @importFrom glue glue
#' @importFrom workflows is_trained_workflow
#'
#' @return a model object
#'
#' @export
load_workflow <- function(workflow_fp) {
  pkg_tic()
# Check data_fp
  stopifnot("workflow_fp must be a filepath" = is.character(workflow_fp),
            "workflow_fp must exist" = file.exists(workflow_fp))

  pkg_messenger(glue::glue("Reading in {workflow_fp}..."))
  workflow_list <- readRDS(workflow_fp)
  pkg_messenger(glue::glue("Read in {workflow_fp}."))

  pkg_messenger(glue::glue("Extracting fitted model from workflow..."))
  to_return <- workflow_list$fitted_wf
  pkg_messenger(glue::glue("Extracted fitted model."))

  # Confirm the object is a trained workflow
  stopifnot("workflow_fp must be a trained workflow" =
              workflows::is_trained_workflow(to_return))
  pkg_toc()

  to_return
}
