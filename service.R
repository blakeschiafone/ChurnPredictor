library(plumber)

r <- plumb("main.R")
r$run(host = '0.0.0.0', port = 8081)
