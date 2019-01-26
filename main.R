source('bq_queries.R')
source('processing.R')
source('models.R')
source('bq.R')
source('utils.R')

suppressPackageStartupMessages(library(readr))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(data.table))
suppressPackageStartupMessages(library(methods))
suppressPackageStartupMessages(library(plumber))
suppressPackageStartupMessages(library(bigrquery))
suppressPackageStartupMessages(library(glue))
suppressPackageStartupMessages(library(FFTrees))
suppressPackageStartupMessages(library(reticulate))
reticulate::source_python("gcloud_ds.py")

Sys.setenv(
  PROJECT = 'tn-devel',
  BQ_AUTH = 'tndevel_auth.json',
  BQ_DATASET = 'kahuna_churn_predictor',
  GS_BUCKET = 'churnpredictor',
  LOCAL_PATH = './',
  MIN_BQ_ROWS = 10000,
  MIN_BQ_USER = 1000,
  JOB_EVENT = '',
  JOB_SESSION = ''
)
set_service_token(Sys.getenv('BQ_AUTH'))

# on flex start-up, run a query to get the latest model definitions and cache results
cached_models <<- query_exec(query = query.models(), project = Sys.getenv('PROJECT'), use_legacy_sql = TRUE)
cached_models <<- data.table::as.data.table(cached_models)
attr(cached_models, "created") <- Sys.time()


#* @get /train
#* @serializer unboxedJSON
get_imp_events <- function(k_namespace, k_onboarding, k_retention){
  bq <- new("QueryBQ", namespace = k_namespace, project = Sys.getenv('PROJECT'))
  insert_job(bq, query = query.user_session(namespace = k_namespace, onboarding = k_onboarding, retention = k_retention), type = 'session')
  insert_job(bq, query = query.user_event(namespace = k_namespace, onboarding = k_onboarding, retention = k_retention), type = 'event')
  extract_job(bq, dest_table = Sys.getenv('BQ_DATASET'), gs_bucket = Sys.getenv('GS_BUCKET'), local_folder = 'session')
  unzip_combine_file(bq, local_folder = 'session')
  extract_job(bq, dest_table = Sys.getenv('BQ_DATASET'), gs_bucket = Sys.getenv('GS_BUCKET'), local_folder = 'event')
  unzip_combine_file(bq, local_folder = 'event')

  churn_data <- new("ChurnData", namespace = k_namespace, onboard_return = c(k_onboarding, k_retention))
  churn_data@session_data <- read_data(churn_data, local_path = '', local_folder = 'session')
  churn_data@event_data <- read_data(churn_data, local_path = '', local_folder = 'event')
  churn_data@event_data <- check_user_count(churn_data, user_column = 'user_id')
  churn_data@event_data <- reshape_events(churn_data)
  churn_data@joined_data <- join_tables(churn_data)
  churn_data@joined_data <- filter_joined(churn_data)
  churn_data@joined_data <- rebalance_table(churn_data)
  churn_data@joined_data <- high_low_flag(churn_data)

  model <- new("FFTModel", data = churn_data@joined_data, namespace = k_namespace)
  model@events_all <- popular_events(churn_data@event_data)
  model@events_imp <- important_events(churn_data@joined_data, model@events_all)
  model@tree <- run_model(model)
  model@best_tree <- best_model(model)
  model@tree_stats <- model_stats(model)
  model@tree_def <- model_definitions(model, tree_number = model@best_tree)
  stats <- create_stats(model, churn_data)

  #update BQ
  create_table(project = Sys.getenv('PROJECT'), dataset = Sys.getenv('BQ_DATASET'), table = k_namespace)
  insert_data_table(churn_data, project = Sys.getenv('PROJECT'), dataset = Sys.getenv('BQ_DATASET'), table = k_namespace, data = stats)
  
  #update datastore
  py_response <- put_model(project = Sys.getenv('PROJECT'), 
                           namespace = k_namespace, 
                           onboard_period = k_onboarding, 
                           return_period = k_retention, 
                           defs = Filter(Negate(is.na), model@tree_def) %>% as.list(.),
                           med_lifetime = median(as.numeric(Sys.Date() - (churn_data@joined_data$begin_onboarding)), na.rm = TRUE),
                           mean_lifetime = mean(as.numeric(Sys.Date() - (churn_data@joined_data$begin_onboarding)), na.rm = TRUE))
  
  return(response_code(
    status = py_response, 
    namespace = slot(churn_data, "namespace"), 
    onboarding = slot(churn_data, "onboard_return")[1], 
    return = slot(churn_data, "onboard_return")[2])
  )
}


#* @get /model_bq
#* @serializer unboxedJSON
get_model_bq <- function(k_namespace, k_onboarding, k_retention){
  check_model_exists(k_namespace, k_onboarding, k_retention)
}


#* @get /model_ds
#* @serializer unboxedJSON
get_model_ds <- function(k_namespace, k_onboarding, k_retention){
  response <- get_model(Sys.getenv('PROJECT'), 
                        glue::glue('{namespace}_{onboard}_{return}', namespace = k_namespace, onboard = k_onboarding, return = k_retention),
                        k_namespace)
  
  return(response)
  
}


#* @get /update_cache
#* @serializer unboxedJSON
get_update_cache <- function(){
  old_model <- cached_models
  cached_models <<- query_exec(query = query.models(), project = Sys.getenv('PROJECT'), use_legacy_sql = FALSE)
  cached_models <<- data.table::as.data.table(cached_models)
  attr(cached_models, "created") <- Sys.time()
  
  if(attributes(cached_models)$created > attributes(old_model)$created){
    response <- response_code(status = 200)
    rm(old_model)
  } else {
    response <- response_code(status = 500)
  }
  
  return(response)
}
