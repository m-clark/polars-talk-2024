# Setup -------------------------------------------------------

library(tidyverse)
library(data.table)
library(collapse)


df = arrow::read_parquet('data/df.pq')

df_join_1 = arrow::read_parquet('data/df_join_1.pq')
df_join_2 = arrow::read_parquet('data/df_join_2.pq')

dt = data.table(df)
dt_join_1 = data.table(df_join_1)
dt_join_2 = data.table(df_join_2)


# Read Operations ----------------------------------------------

read_operation <- function(pkg, path, ...) {
    if (pkg == 'data.table') {
        return(data.table::fread(path, ...))
    } else if (pkg == 'dplyr') {
        return(arrow::read_parquet(path, ...))
    } else if (pkg == 'special') {
        # Add code for special package read operation here
    }
}


# Join Operations ----------------------------------------------
join_operation <- function(pkg, how, ...) {
    if (pkg == 'data.table') {
        if (how == 'inner') {
            return(merge(dt_join_1, df_join_2, ...))
        } else if (how == 'left') {
            return(dt_join_1[dt_join_2, ...])
        } else if (how == 'right') {
            return(dt_join_2[dt_join_1, ...])
        }
    } else if (pkg == 'dplyr') {
        f = ifelse(how == 'inner', dplyr::inner_join,
                   ifelse(how == 'left', dplyr::left_join,
                          ifelse(how == 'right', dplyr::right_join)))  
        return(f(df_join_1, df_join_2, ...))
    } else if (pkg == 'special') {
        # Add code for special package join operation here
    }
}



# group_by Operation -------------------------------------------

group_by_operation <- function(pkg, with_lambda = FALSE, ...) {
    if (pkg == 'data.table') {
        if (with_lambda) {
            res = dt[, .(x_mean = mean(x)^2, y_sum = sum(y)/10), by = c('grp', 'grp_3')]
        }
        else {
            res = dt[, .(x_mean_sq = mean(x), y_sum_10 = sum(y)), by = c('grp', 'grp_3')]
        }
        return(res)
    } else if (pkg == 'dplyr') {
        if (with_lambda) {
            res = df |> 
                summarize(
                    x_mean_sq = mean(x)^2,   # there is not a direct way to do this in terms of passing a function because it's never necessary we can create functions and use those.
                    y_sum_10  = sum(y)/10,
                    .by = c(grp, grp_3)
                )
        }
        else {
            res = df |> 
                summarize(
                    x_mean = mean(x),
                    y_sum  = sum(y),
                    .by = c(grp, grp_3)
                )
        }
        return(res)
    } else if (pkg == 'special') {
        if (with_lambda) {
            res = df %>%
            fgroup_by(grp, grp_2) %>%
            fsummarise(mean_x = fmean(x)^2, sum_y = fsum(y)/10)
        }
        else {
            res = df %>%
            fgroup_by(grp, grp_2) %>%
            fsummarise(mean_x = fmean(x), sum_y = fsum(y))
        }

    }
}


# Testing -----------------------------------------------------

# read_operation('dplyr', 'data/df.pq')
# read_operation('data.table', 'data/df.csv')

# join_operation('dplyr', by = c('grp', 'grp_2'), how = 'right')
# join_operation('data.table', on = c('grp', 'grp_2'), how = 'right')


# group_by_operation('dplyr', with_lambda = TRUE)
# group_by_operation('data.table', with_lambda = TRUE)
# group_by_operation('data.table', with_lambda = FALSE)



# Benchmarking ------------------------------------------------

library(microbenchmark)

gb_results_raw = microbenchmark(
    tidy         = group_by_operation('dplyr'),
    data_table   = group_by_operation('data.table'),
    secretRpower = group_by_operation('special'),
    tidy_lam         = group_by_operation('dplyr', with_lambda = TRUE),
    data_table_lam   = group_by_operation('data.table', with_lambda = TRUE),
    secretRpower_lam = group_by_operation('special', with_lambda = TRUE),
    times = 50,
    unit  = 'ms'
)

print(gb_results_raw)

gb_results = gb_results_raw |> 
    summary() |>  
    as_tibble() |> 
    mutate(
        operation = 'gb',
        n_reps    = 50,
        time_format = 'ms',
        setting   = rep(c('lambda-false', 'lambda-true'), each = 3),
    )

read_results_raw = microbenchmark(
    tidy  = read_operation('dplyr', 'data/df.pq'),
    times = 50,
    unit  = 'ms'
)

print(read_results_raw)

read_results = read_results_raw |> 
    summary() |> 
    as_tibble() |> 
    mutate(
        operation = 'read',
        n_reps    = 50,
        time_format = 'ms',
        setting   = 'NA'
    )

join_results_raw = microbenchmark(
    tidy_left  = join_operation('dplyr', by = c('grp', 'grp_2'), how = 'left'),
    tidy_inner = join_operation('dplyr', by = c('grp', 'grp_2'), how = 'inner'),
    tidy_right = join_operation('dplyr', by = c('grp', 'grp_2'), how = 'right'),
    dt_left    = join_operation('data.table', on = c('grp', 'grp_2'), how = 'left'),
    dt_inner   = join_operation('data.table', on = c('grp', 'grp_2'), how = 'inner'),
    dt_right   = join_operation('data.table', on = c('grp', 'grp_2'), how = 'right'),
    times = 5,
    unit = 'ms'
) 

print(join_results_raw)

join_results = join_results_raw |> 
    summary() |> 
    as_tibble() |> 
    mutate(
        operation   = 'join',
        n_reps      = 50,
        time_format = 'ms',
        setting     = rep(c('left', 'inner', 'right'), times = 2),
    )


# Save

bind_rows(
    gb_results,
    read_results,
    join_results
) |> 
    arrow::write_parquet('data/bench_results_r.pq'
)
