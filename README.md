# polars-talk-2024

![polars_logo](https://raw.githubusercontent.com/m-clark/polars-talk-2024/main/polars_logo_white.png)


Notebook and benchmarks for a 1 hour demo of Polars. Includes:

- `polars.ipynb`: Jupyter notebook with Polars examples
- `data/`: Data files used in the notebook
- `polars_benchmarks.ipynb`: Jupyter notebook for an interactive approach
- `bench.py/r`: Actual files used for presented results


Some very ungeneralizable/simple benchmarks:

- read_parquet: 2M rows < 10 columns (R used the `arrow` package)
- group by: simple mean and sum vs. a 'lambda' function using the mean and sum
- joins: very small subsets of the 2M row dataset (10k vs. 4k)

All runs were executed by running `python bench.py` or `Rscript bench.R` in the terminal. Both are included in the repo. The results are shown below.  The data used in the benchmark is created with `data/data_setup.r` and is not in the repo.

![bench_results](https://raw.githubusercontent.com/m-clark/polars-talk-2024/main/bench_results.png)
