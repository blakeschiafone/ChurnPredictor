QueryBQ = setClass(
  "QueryBQ",
  
  slots = c(
    namespace = "character",
    project = "character",
    job_event = "character",
    job_session = "character"
  )
)

setMethod(f = "initialize", signature = "QueryBQ",
          definition = function(.Object, namespace = '', project = '') {
            .Object@namespace <- namespace
            .Object@project <- project
            .Object@job_event <- ''
            .Object@job_session <- ''
            return(.Object)
          }
)

setGeneric(name = "insert_job", def = function(self, query, dest_table = Sys.getenv('BQ_DATASET'), type = 'event',
                                                 write_disposition = 'WRITE_TRUNCATE') standardGeneric("insert_job"))
setMethod(f = "insert_job", signature = "QueryBQ",
          definition = function(self, query, dest_table, type, write_disposition) {
            job <- insert_query_job(project = self@project, 
                                    query = query, 
                                    destination_table = glue::glue('{project}:{dest_table}.{namespace}_{type}', 
                                                                   project = self@project, namespace = self@namespace, type = type, dest_table = dest_table), 
                                    write_disposition = write_disposition,
                                    use_legacy_sql = FALSE)
            
            val <- list(parse_bq_job(job))
            names(val) <- toupper(glue::glue('job_{type}', type = type))
            do.call(Sys.setenv, val)
            # if (type == 'event'){
            #   bq@job_event <<- parse_bq_job(job)
            # } else if (type == 'session'){
            #   bq@job_session <<- parse_bq_job(job)
            # }
            
            #wait_for(job, pause = 4)
          }
)

setGeneric(name = "extract_job", def = function(self, dest_table = Sys.getenv('BQ_DATASET'), gs_bucket = Sys.getenv('GS_BUCKET'), 
                                                local_path = Sys.getenv('LOCAL_PATH'), local_folder) standardGeneric("extract_job"))
setMethod(f = "extract_job", signature = "QueryBQ",
          definition = function(self, dest_table, gs_bucket, local_path, local_folder) {
            if(!exists_gcs_bucket(gs_bucket)){
              message(glue::glue("{gs_bucket} does not exist.  creating", gs_bucket = gs_bucket))
              
              tryCatch({
                create_gcs_bucket(gs_bucket)
              }, error = function(e) {
                stderr(glue::glue("unable to create GS bucket: {gs_bucket}", gs_bucket = gs_bucket))
                stop()
              })
            }
            
            # check for either job_event or job_session completion
            val <- toupper(glue::glue('job_{type}', type = local_folder))
            check_bq_job(project = Sys.getenv('PROJECT'), job_id = Sys.getenv(val))
            # if (local_folder == 'event'){
            #   check_bq_job(project = Sys.getenv('PROJECT'), job_id = bq@job_event)
            # } else if (local_folder == 'session'){
            #   check_bq_job(project = Sys.getenv('PROJECT'), job_id = bq@job_session)
            # }
            
            print('extracting data')
            invisible(system(glue::glue("gsutil rm gs://{gs_bucket}/{namespace}/{local_folder}/*", 
                              gs_bucket = gs_bucket, namespace = self@namespace, local_folder = local_folder)))
            invisible(system(glue::glue("mkdir {local_path}{namespace}", 
                              local_path = local_path, namespace = self@namespace)))
            invisible(system(glue::glue("rm {local_path}{namespace}/{local_folder}/*", 
                              local_path = local_path, namespace = self@namespace, local_folder = local_folder)))
            invisible(system(glue::glue("bq extract --compression=GZIP '{project}:{dest_table}.{namespace}_{local_folder}' gs://{gs_bucket}/{namespace}/{local_folder}/_*.csv.gz", 
                              project = self@project, dest_table = dest_table, gs_bucket = gs_bucket, namespace = self@namespace, local_folder = local_folder)))
            invisible(system(glue::glue("gsutil -m cp -R gs://{gs_bucket}/{namespace}/{local_folder}/ {local_path}{namespace}/", 
                              gs_bucket = gs_bucket, namespace = self@namespace, local_folder = local_folder, local_path = local_path)))
          }
)

setGeneric(name = "unzip_combine_file", def = function(self, local_path = Sys.getenv('LOCAL_PATH'), local_folder) standardGeneric("unzip_combine_file"))
setMethod(f = "unzip_combine_file", signature = "QueryBQ",
          definition = function(self, local_path, local_folder) {
            system(glue::glue("gunzip {local_path}{namespace}/{local_folder}/*.csv.gz",
                              local_path = local_path, namespace = self@namespace, local_folder = local_folder))
            system(glue::glue("head -1 {local_path}{namespace}/{local_folder}/_000000000000.csv > {local_path}{namespace}/{local_folder}/combined.csv; tail -n +2 -q {local_path}{namespace}/{local_folder}/_*.csv >> {local_path}{namespace}/{local_folder}/combined.csv",
                              local_path = local_path, namespace = self@namespace, local_folder = local_folder))
            system(glue::glue("find {local_path}{namespace}/{local_folder}/ -type f ! -name 'combined.csv' -delete",
                              local_path = local_path, namespace = self@namespace, local_folder = local_folder))
          }
)


create_gcs_bucket <- function(gs_bucket = Sys.getenv('GS_BUCKET')){
  response <- system(glue::glue("gsutil mb gs://{gs_bucket}", gs_bucket = gs_bucket), ignore.stderr = TRUE)
  response <- ifelse(response == 0, TRUE, FALSE)

  return(response)
}


create_data_set <- function(project = Sys.getenv('PROJECT"'), dataset = Sys.getenv('BQ_DATASET')){
  if (exists_data_set(project, dataset)){
    message(glue::glue("{dataset} in project {project} already exists.  Not creating.", dataset = dataset, project = project))
  } else {
    bigrquery::insert_dataset(project = project, dataset = dataset)
  }
}


create_table <- function(project = Sys.getenv('PROJECT"'), dataset = Sys.getenv('BQ_DATASET'), table = '8tracks'){
  if (exists_table(project, dataset, table)){
    message(glue::glue("{table} in dataset {dataset} for project {project} already exists.  Not creating", 
                       table = table, dataset = dataset, project = project))
  } else {
    message(glue::glue("Creating {table} in {dataset}", table = table, dataset = dataset))
    bigrquery::insert_table(project = project, dataset = dataset, table = table)
    }
}


exists_gcs_bucket <- function(gs_bucket = Sys.getenv('GS_BUCKET')){
  response <- invisible(system(glue::glue("gsutil ls gs://{gs_bucket}", gs_bucket = gs_bucket), ignore.stderr = TRUE, ignore.stdout = TRUE))
  response <- ifelse(response == 0, TRUE, FALSE)
  
  return(response)
}


exists_data_set <- function(project = Sys.getenv('PROJECT"'), dataset = Sys.getenv('BQ_DATASET')){
  sets <- bigrquery::list_datasets(project, page_size = 1000, max_pages = Inf)
  
  if (dataset %in% sets){
    return(TRUE)
  } else {
    return(FALSE)
  }
}


exists_table <- function(project = Sys.getenv('PROJECT"'), dataset = Sys.getenv('BQ_DATASET'), table = '8tracks'){
  tables <- bigrquery::list_tables(project, dataset, page_size = 1000, max_pages = Inf)
  
  if (table %in% tables){
    return(TRUE)
  } else {
    return(FALSE)
  }
}


parse_bq_job <- function(response) {
  tryCatch({
    response$jobReference$jobId
  }, error = function(e) {
    stderr(glue::glue("no job ID to parse in response object"))
    stop()
    })
}


check_bq_job <- function(project, job_id) {
  tryCatch({
    response <- invisible(bigrquery::get_job(project, job = job_id))
    while(response$status$state != "DONE") {
      Sys.sleep(2)
      response <- invisible(bigrquery::get_job(project, job = job_id))
    }
    
    status <- response$status$state
    if (status == 'DONE') {
      return(1)
    }
  }, error = function(e) {
    return(0)
    stderr(glue::glue("job_id {job_id} failed for {namespace}",job_id = job_id, namespace = bq@namespace))
    stop()
  })
}


insert_data_table <- function(churn_data, project = 'tn-devel', dataset = 'kahuna_churn_predictor', table = '8tracks', data){
  if (!any(class(data) %in% c('data.frame', 'matrix'))){
        message(glue::glue("Attempting to write a non data.frame/matrix class to bigquery.  Not writing"))
    } else if (exists_data_set(project, dataset) == FALSE) {
        message(glue::glue("{dataset} in project {project} does not exist.  Not writing data"))
    } else if (exists_table(project, dataset, table) == FALSE) {
        message(glue::glue("{table} in dataset {dataset} for project {project} does not exist.  Not writing data"))
    } else {
        message(glue::glue("Uploading model for {table} in {dataset}", table = table, dataset = dataset))
        response <- bigrquery::insert_upload_job(project, dataset, table , values = data, write_disposition = 'WRITE_APPEND')
        response <- check_bq_job(project = project, job_id = parse_bq_job(response))
        
        if (response == 1){
          message(glue::glue("Successfully wrote {namespace} data to bigquery", namespace = table))
        } else {
          stderr(glue::glue("Failed to write {namespace} data to bigquery", namespace = table))
          stop()
        }
    }
}