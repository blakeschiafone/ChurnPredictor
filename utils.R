response_code <- function(status, ...){
  if(status == 200){
    response <- list(
      status = "SUCCESS",
      code = 200,
      output = list(...)
    )
  } else if (status == 500){
    response <- list(
      status = "ERROR",
      code = 500,
      output = list(...)
    )
  }
  response <- jsonlite::toJSON(response, auto_unbox = TRUE)
  return(response)
}


std_err <- function(response){
  write(response, stderr())
  stop()
}


std_out <- function(response){
  write(response, stdout())
}