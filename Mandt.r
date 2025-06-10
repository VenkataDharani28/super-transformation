# Messaging ####
#' Package messenger function for sending messages
#'
#' `pkg_messenger` is a custom version of `message`, but is intended to be
#' re-written by PCML to align with their logger.
#'
#' @inheritParams base::message
#'
#' @return a model object
#'
#' @export
pkg_messenger <- function(..., domain = NULL, appendLF = TRUE) {
  # PCML Can change this....
  message(..., domain = domain, appendLF = appendLF)
}# Timing ####
#' Messenger tic start
#'
#' Notes start of msg and begins tictoc::toc(msg)
#'
#' @importFrom glue glue
#' @importFrom tictoc tic
#'
#' @return entry in messenger log
#'
#' @export
pkg_tic <- function() {
  # Get name of calling function
  msg <- sys.call(-1L)[1L]

  # Send a message about it
  pkg_messenger(glue::glue("START func: {msg}"))

  # Start timer
  tictoc::tic(msg)
}


#' Messenger log
#'
#' Notes time from current tictoc::tic for the log
#'
#' @importFrom glue glue
#' @importFrom tictoc toc
#'
#' @return entry in messenger log
#'
#' @export
pkg_toc <- function() {
  tictoc::toc(quiet = TRUE, log = TRUE)
}
#' Messenger messages
#'
#' Prepares and prints times from tictoc log. Intent is to call this function
#' at the end of a set of internal functions.
#'
#' @importFrom tictoc toc
#'
#' @return entry in messenger log
#'
#' @export
pkg_toc_exit <- function() {
  # One last call to end timer
  pkg_toc()

  # Create message
  msg_log <- paste(tictoc::tic.log(), collapse = "\n")

  # Clear tictoc
  tictoc::tic.clear()
  tictoc::tic.clearlog()

  # Message
  pkg_messenger(msg_log)

}
