ChurnData = setClass(
  "ChurnData",
  
  slots = c(
    namespace = "character",
    onboard_return = "vector",
    segment_cuts = "ANY",
    session_data = "data.frame",
    event_data = "data.frame",
    joined_data = "data.frame",
    joined_data_orig = "data.frame"
  )
)

setMethod(f = "initialize", signature = "ChurnData",
          definition = function(.Object, namespace = '', onboard_return = c(), segment_cuts = NULL, session_data = data.frame(), event_data = data.frame(), 
                                joined_data = data.frame(), joined_data_orig = data.frame()) {
            .Object@namespace <- namespace
            .Object@onboard_return <- onboard_return
            .Object@segment_cuts <- segment_cuts
            .Object@session_data <- session_data
            .Object@event_data <- event_data
            .Object@joined_data <- joined_data
            .Object@joined_data_orig <- joined_data_orig
            return(.Object)
          }
)

setGeneric("read_data", function(self, local_path, local_folder) standardGeneric("read_data"))
setMethod(f = "read_data", signature = "ChurnData",
          definition = function(self, local_path, local_folder) {
              data <- data.table::fread(glue::glue("{local_path}{namespace}/{local_folder}/combined.csv", 
                                                   local_path = local_path, local_folder = local_folder, namespace = self@namespace), 
                                        data.table = TRUE, showProgress = FALSE, verbose = FALSE)
              
              message(glue::glue("{namespace} {local_folder} data ingested", namespace = self@namespace, local_folder = local_folder))
            
            return(data)
          }
)

setGeneric("check_user_count", function(self, user_column = 'user_id', val_greater_than = 1000000, sample_users = 900000) standardGeneric("check_user_count"))
setMethod(f = "check_user_count", signature = "ChurnData",
          definition = function(self, user_column, val_greater_than, sample_users) {
            data <- data.table::data.table(self@event_data, key = user_column)
            if (length(unique(data[[user_column]])) > val_greater_than){
              samp_userid <- sample(unique(data[[user_column]]), sample_users, replace = FALSE)
              data_rows <- data[J(samp_userid)]
              data <- data_rows
            }
            
            return(data)
          }
) 

setGeneric("reshape_events", function(self, rename_col = 'event', select_filter = 'return_period == "onboarding"', 
                                     select_columns = c('user_id', 'event', 'event_count'), 
                                     cast_x = 'user_id', cast_y = 'event', cast_val = 'event_count') standardGeneric("reshape_events"))
setMethod(f = "reshape_events", signature = "ChurnData",
          definition = function(self, rename_col, select_filter, select_columns, 
                                cast_x, cast_y, cast_val) {
            self@event_data[[rename_col]] <- gsub(" ", "_", self@event_data[[rename_col]])
            data_spread <- self@event_data[eval(parse(text=select_filter)), select_columns, with = F] %>% 
              select(select_columns) %>% 
              data.table::dcast(., as.formula(glue::glue("{cast_x} ~ {cast_y}", cast_x = cast_x, cast_y = cast_y)), value.var = cast_val, fill = 0)
            
            names(data_spread) <- gsub("`", "", names(data_spread))
            names(data_spread) <- gsub("-| ", "_", names(data_spread))
            
            message(glue::glue("{namespace} data reshaped", namespace = self@namespace))
            return(data_spread)
          }
)

setGeneric("join_tables", function(self, left_join = FALSE) standardGeneric("join_tables"))
setMethod(f = "join_tables", signature = "ChurnData",
          definition = function(self, left_join){
            user_event_joined <- if(left_join == FALSE){
              self@event_data[self@session_data, on = 'user_id']
            } else {
                self@session_data[self@event_data, on = 'user_id']
            }
            
            check_model_data(user_event_joined)
            message(glue::glue("{namespace} data joined", namespace = self@namespace))
            
            return(user_event_joined)
          }
)

setGeneric("filter_joined", function(self, remove_users_before = "2017-10-01") standardGeneric("filter_joined"))
setMethod(f = "filter_joined", signature = "ChurnData",
          definition = function(self, remove_users_before) {
            self@joined_data_orig <- self@joined_data
            data <- self@joined_data[self@joined_data$onboarding > 1]
            data <- data[data[, .I[!is.na(.SD)], .SDcols = 2]]  # remove records where all rows == NA
            data$returned <- ifelse(data$return_period == 0 & data$onboarding != 0, FALSE, 
                                    ifelse(data$session_minutes > (data$onboarding + data$return_period), TRUE, 'ignore'))
            data$begin_onboarding <- as.Date(data$begin_onboarding)
            data <- data[begin_onboarding >= as.Date(remove_users_before),]
            data <- data[begin_onboarding <= Sys.Date() - 31,]
            data <- data[!is.na(device_type),]
            data$onboarding_round <- plyr::round_any(data$onboarding, 5)
            filtered_users <<- length(data$onboarding_round) # TODO Remove
            
            return(data)
          }
)

setGeneric("rebalance_table", function(self, rebalance_flag = .2, rebalance_new = .5, rebalance_col = "returned") standardGeneric("rebalance_table"))
setMethod(f = "rebalance_table", signature = "ChurnData",
          definition = function(self, rebalance_flag, rebalance_new, rebalance_col) {
            class_prop <- self@joined_data[, .(count = .N), by = .(returned)] %>% 
              mutate(total = count / sum(count)) %>% 
              filter(returned == FALSE) %>% 
              select(total)
            
            rebalance_col_idx <- which(names(self@joined_data) == rebalance_col)
            per_value <- ifelse(class_prop[[1]] < rebalance_flag, rebalance_new, class_prop[[1]]) * 100

            if(per_value == (rebalance_new * 100)){
              rebalanced_data <- self@joined_data[returned != "ignore"]
              rebalanced_data[[rebalance_col_idx]] <- as.logical(rebalanced_data[,returned])
              rebalanced_data <- unbalanced::ubOver(X = subset(rebalanced_data, select = -eval(parse(text=rebalance_col))), Y = rebalanced_data[[rebalance_col]])
              rebalanced_data <- cbind(rebalanced_data$X, rebalanced_data$Y)
              names(rebalanced_data)[length(names(rebalanced_data))] <- "returned"
              message(glue::glue("{namespace} data rebalanced because churned percentage was {churned_perc}", 
                                 namespace = self@namespace, churned_perc = class_prop[[1]]))
            } else {
              rebalanced_data <- self@joined_data[returned != "ignore"]
              message(glue::glue("{namespace} did not rebalance", namespace = self@namespace))
            }
            
            return(rebalanced_data)
          }
)

setGeneric("high_low_flag", function(self, grouping_val = "returned", sum_val = "onboarding", sum_quantile = .5) standardGeneric("high_low_flag"))
setMethod(f = "high_low_flag", signature = "ChurnData",
          definition = function(self, grouping_val, sum_val, sum_quantile) {
            median_val <- self@joined_data %>% 
              group_by_(grouping_val) %>% 
              summarize_(median = lazyeval::interp(~quantile(sum_val, sum_quantile), sum_val = as.name(sum_val)))
            
            segment_cuts <<- c('high' = median_val$median[median_val$returned == TRUE], 
                                      'low' = median_val$median[median_val$returned == FALSE])
            # slot(self, "segment_cuts") <- segment_cuts
            self@joined_data$high_low <- ifelse(self@joined_data$returned == TRUE & self@joined_data$onboarding > median_val$median[median_val$returned == TRUE], 'high', 
                                    ifelse(self@joined_data$returned == FALSE & self@joined_data$onboarding < median_val$median[median_val$returned == FALSE], 'low', 
                                           'ignore'))
            data <- self@joined_data[returned %in% c(TRUE, FALSE),]
            data$returned <- as.logical(data$returned)
            
            message(glue::glue("{namespace} high_low flag created", namespace = self@namespace))
            return(data)
          }
)


check_model_exists <- function(k_namespace, k_onboard, k_return){
  response <- k_namespace %in% cached_models$namespace
  
  if(response == FALSE){
    stderr(glue::glue("a model for {namespace} does not exist", namespace = k_namespace))
    stop()
  }
  
  response <- cached_models[namespace == k_namespace & onboarding == k_onboard & return == k_return]
  if(nrow(response) == 0){
    stderr(glue::glue("{namespace} has a model, but k_onboarding: {k_onboard} and k_retention: {k_return} do not exist",
                       namespace = k_namespace, k_onboard = k_onboard, k_return = k_return))
    response <- FALSE
  }
  
  if(response == FALSE){
    response <- response_code(status = 500, namespace = k_namespace, onboarding = k_onboard, return = k_return)
  } else {
    response <- get_model_defs(k_namespace, k_onboard, k_return)
    response <- response_code(status = 200, namespace = k_namespace, defs = list(response), onboarding = k_onboard, return = k_return)
  }
  
  return(response)
}


get_model_defs <- function(k_namespace, k_onboard, k_return){
  response <- cached_models[namespace == k_namespace & onboarding == k_onboard & return == k_return, def_1:def_9]
}


check_cache_exists <- function(){
  response <- FALSE
  if(exists('cached_models') & nrow(cached_models) > 0){
    response <- TRUE
  }
  
  return(response)
}


check_model_data <- function(data){
  if (length(data$user_id) < Sys.getenv('MIN_BQ_ROWS')){
    stderr(glue::glue('{namespace} bigquery results not greater than {min_bq_results}. stopping.', 
                       namespace = bq@namespace, min_bq_results = Sys.getenv('MIN_BQ_RESULTS')))
    stop()
  }
}


popular_events <- function(file){
  keep_events <- file %>% 
    select(-user_id) %>% 
    summarize_all(., .funs = sum) %>% 
    mutate(total = rowSums(.)) %>% 
    tidyr::gather(key, value, -total) %>% 
    mutate(perc = value / total, med = median(value) / sum(value)) %>% 
    select(-c(total, value)) %>% 
    filter(perc > med) %>% 
    select(key) %>% 
    as.list(.)
  
  return(keep_events[[1]])
}


function_read_data <- function(namespace, local_path = '/home/rstudio/scripts/data_science/onboarding/data/', local_folder = 'session'){
  data <- list.files(glue::glue("{local_path}{namespace}/{local_folder}/", 
                                local_path = local_path, local_folder = local_folder, namespace = namespace),
                     full.names = TRUE) %>% 
    purrr::map_df(~ data.table::fread(paste0('zcat ', .), data.table = TRUE, showProgress = FALSE))
  if(exists(local_folder)){rm(list = local_folder, pos = 1)}
  assign(glue::glue("{local_folder}", local_folder = local_folder), data, pos = 1)
}