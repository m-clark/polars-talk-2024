library(tidyverse)

# Generate random data
set.seed(123)
n <- 2000000

df <- tibble(
    x     = rnorm(n),
    y     = rbinom(n, 1, 0.75),
    grp   = rep(1:10000, each = n/10000), # more groups
    grp_2 = rep(1:100, each = n/100),     # fewer groups
    idx1  = rep(1:(n/10000), 10000),      # index e.g. time
    idx2  = rep(1:(n/100), 100),
) |> 
    mutate(
        grp_3 = sample(letters[1:5], n(), replace = TRUE), # 3 groups
        .by = grp
    ) |> 
    relocate(grp_3, .after = grp_2)

arrow::write_parquet(df, "data/df.pq")
# write_csv(df, "data/df.csv")

# for joining; note that slice_ does not use seed.
df_1 = df |> 
    distinct(grp, grp_2)

df_2 = df |> 
    distinct(grp, grp_2) |> 
    slice_sample(prop = .40) 

df_1 |> left_join(df_2,  by = c("grp", "grp_2")) 
df_1 |> inner_join(df_2, by = c("grp", "grp_2"))
df_1 |> right_join(df_2, by = c("grp", "grp_2"))

arrow::write_parquet(df_1, "data/df_join_1.pq")
arrow::write_parquet(df_2, "data/df_join_2.pq")