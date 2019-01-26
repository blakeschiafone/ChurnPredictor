FFTModel = setClass(
  "FFTModel",
  
  slots = c(
    data = "data.frame",
    namespace = "character",
    events_all = "ANY",
    events_imp = "ANY",
    train_perc = "numeric",
    goal = "character",
    sens_weight = "numeric",
    tree = "ANY",
    best_tree = "numeric",
    tree_def = "data.frame",
    tree_stats = "list"
  )
)

setMethod(f = "initialize", signature = "FFTModel", 
          definition = function(.Object, data = data.frame(), namespace = '', events_all = NULL, events_imp = NULL, train_perc = .7, goal = 'wacc', 
                                sens_weight = .5, tree = list(), best_tree = 0, tree_def = data.frame(), tree_stats = list()) {
            .Object@data <- data
            .Object@namespace <- namespace
            .Object@events_all <- events_all
            .Object@events_imp <- events_imp
            .Object@train_perc <- train_perc
            .Object@goal <- goal
            .Object@sens_weight <- sens_weight
            .Object@tree <- tree
            .Object@best_tree <- best_tree
            .Object@tree_def <- tree_def
            .Object@tree_stats <- tree_stats
            return(.Object)
          }
)

setGeneric(name = "run_model", def = function(self) standardGeneric("run_model"))
setMethod(f = "run_model", signature = "FFTModel",
          definition = function(self) {
            if (length(self@events_imp) > 0){
              fft <- FFTrees::FFTrees(formula = as.formula(paste("returned ~ onboarding_round + ", paste(self@events_imp, collapse = " + "), sep = "")), 
                                      data = self@data, train.p = self@train_perc, goal = self@goal, sens.w = self@sens_weight, 
                                      do.comp = FALSE, do.cart = FALSE, do.lr = FALSE, do.rf = FALSE, do.svm = FALSE, 
                                      main = "Return", decision.labels = c('churned', 'returned'))
            } else {
              fft <- FFTrees::FFTrees(formula = as.formula("returned ~ onboarding_round"), 
                                      data = self@data[, .(returned, onboarding_round)], train.p = self@train_perc, goal = self@goal, sens.w = self@sens_weight, 
                                      do.comp = FALSE, do.cart = FALSE, do.lr = FALSE, do.rf = FALSE, do.svm = FALSE, 
                                      main = "Return", decision.labels = c('churned', 'returned'))
            }
            
            message(glue::glue("FFT model for {namespace} created", namespace = self@namespace))
            
            return(fft)
          }
)

setGeneric(name = "best_model", def = function(self) standardGeneric("best_model"))
setMethod(f = "best_model", signature = "FFTModel",
          definition = function(self) {
            trees <- self@tree$tree.stats$test %>% 
              select(sens, spec, bacc, acc) %>% 
              mutate(sens_spec = sens + spec, wacc = acc - bacc, score = sens_spec + wacc)
            
            max_tree <- which(trees$score == max(trees$score))[1]
            
            message(glue::glue("Best model for {namespace} found", namespace = self@namespace))
            return(max_tree)
          }
)

setGeneric(name = "model_stats", def = function(self) standardGeneric("model_stats"))
setMethod(f = "model_stats", signature = "FFTModel",
          definition = function(self) {
            results <- FFTrees:::apply.tree(self@data, 
                                  formula = self@tree$formula, 
                                  tree.definitions = self@tree$tree.definitions[self@best_tree,]) %>% .$treestats
            
            message(glue::glue("Model stats for {namespace} created", namespace = self@namespace))
            return(results)
          }
)

setGeneric(name = "model_definitions", def = function(self, tree_number) standardGeneric("model_definitions"))
setMethod(f = "model_definitions", signature = "FFTModel",
          definition = function(self, tree_number) {
            try(if(tree_number > nrow(self@tree$tree.definitions)) stop("tree number selected is greater than available trees"))
            
            definitions <- self@tree$tree.definitions[tree_number,][c('cues', 'directions', 'thresholds')]
            definitions <- apply(definitions, 2, function(x) strsplit(x, ';')[[1]])
            definitions <- as.data.frame(definitions)
            definitions <- mutate(definitions, a = paste0('def_', row_number()), b = paste(cues, directions, thresholds)) %>% select(a:b)
            definitions <- add_row(definitions, a = paste0('def_', (nrow(definitions) + 1):9))
            definitions <- tidyr::spread(definitions, a, b)
            # definitions <- paste(unlist(definitions), collapse = '. ')
            
            message(glue::glue("Definitions for {namespace} created", namespace = self@namespace))
            return(definitions)
          }
)


create_stats <- function(model, churn_data){
  stats <- data.frame(seen_users = length(unique(slot(churn_data, "event_data")$user_id)), 
                      filtered_users = filtered_users, 
                      segmented_users = nrow(slot(model, "data")), 
                      onboard_high = segment_cuts[[1]], 
                      onboard_low = segment_cuts[[2]], 
                      filtered_churned = 1 - mean(as.logical(slot(churn_data, "joined_data")$returned)), 
                      filtered_returned = mean(as.logical(slot(churn_data, "joined_data")$returned)), 
                      churned_test = slot(model, "tree")$tree.stats$test[slot(model, "best_tree"),]$spec, 
                      returned_test = slot(model, "tree")$tree.stats$test[slot(model, "best_tree"),]$sens,
                      churned_filtered = slot(model, "tree_stats")$spec, 
                      returned_filtered = slot(model, "tree_stats")$sens,
                      events_all = slot(model, "events_all") %>% paste(., collapse = ', '), 
                      events_imp = slot(model, "events_imp") %>% paste(., collapse = ', '))
  
  return(cbind(data.frame(table_id = slot(churn_data, "namespace"),
                          modified = Sys.time(), 
                          onboarding = as.numeric(slot(churn_data, "onboard_return")[1]), 
                          return = as.numeric(slot(churn_data, "onboard_return")[2])), 
                          model@tree_def, 
                          user_lifetime_median = round(median(as.numeric(Sys.Date() - (slot(churn_data, "joined_data")$begin_onboarding)), na.rm = TRUE)),
                          user_lifetime_mean = round(mean(as.numeric(Sys.Date() - (slot(churn_data, "joined_data")$begin_onboarding)), na.rm = TRUE)),
                          stats)
         )
}


important_events <- function(file, event_list, glm_x = 'returned'){
  all.glm <- glm(as.formula(glue::glue('{glm_x} ~ .', glm_x = glm_x)), 
                 data = file[high_low != 'ignore', c('returned', 'onboarding_round', 'session_count', event_list), with = FALSE], 
                 family = 'poisson')
  
  all.imp <- caret::varImp(all.glm)  # calculate variable importance
  all_keep.glm <- row.names(all.imp)[order(-all.imp$Overall)[1:5]]  # keep the top 5 variables
  all_keep.glm <- all_keep.glm[!is.na(all_keep.glm)] # remove possible NA's
  all_keep.glm <- setdiff(all_keep.glm, names(which(all.glm$coefficients < 0)))  # remove any features that have negative estimates
  all_keep.glm <- all_keep.glm[!(all_keep.glm %in% c('onboarding_round', 'session_count'))]
  
  if (length(all_keep.glm) == 0) {
    all_keep.glm <- event_list
  }
  
  return(all_keep.glm)
}


tp_prop <- function(file, grouping_val = 'returned', tp = FALSE){
  data <- get(deparse(substitute(file)))
  
  class_prop <- data[, .(count = .N), by = grouping_val] %>% 
    mutate(total = count / sum(count)) %>%
    filter_(substitute(col == val, list(col = as.name(grouping_val), val = tp))) %>% 
    select(total)
  
  return(class_prop[[1]])
}


function_fftmodel <- function(file, glm_output, train_perc = .7, goal = 'wacc', sens_weight = .5){
  data <- get(deparse(substitute(file)))
  imp_events = get(deparse(substitute(glm_output)))
  
  if (length(imp_events) > 0) {
    fft <- FFTrees::FFTrees(formula = as.formula(paste("returned ~ onboarding_round + ", paste(imp_events, collapse = " + "), sep = "")), 
                                       data = data, train.p = train_perc, goal = goal, sens.w = sens_weight, 
                                       do.comp = FALSE, do.cart = FALSE, do.lr = FALSE, do.rf = FALSE, do.svm = FALSE, 
                                       main = "Return", decision.labels = c('churned', 'returned'))
  } else {
    fft <- FFTrees::FFTrees(formula = as.formula("returned ~ onboarding_round"), 
                                       data = data[, .(returned, onboarding_round)], train.p = train_perc, goal = goal, sens.w = sens_weight, 
                                       do.comp = FALSE, do.cart = FALSE, do.lr = FALSE, do.rf = FALSE, do.svm = FALSE, 
                                       main = "Return", decision.labels = c('churned', 'returned'))
  }
  
  return(fft)
}