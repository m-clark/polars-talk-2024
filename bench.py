import polars as pl
import pandas as pd
import numpy as np

import time

df = pd.read_parquet('data/df.pq')

df_join_1 = pd.read_parquet('data/df_join_1.pq')

df_join_2 = pd.read_parquet('data/df_join_2.pq')


class TimedComparison:
    def __init__(
            self,
            df=None,
            df2=None,
            n_reps=25,
            time_format='ms',
            save_output=False
        ):

        self.n_reps = n_reps
        self.time_format = time_format
        self.times = {'pandas': [], 'polars': []}
        self.df = df
        self.df_join_2 = df2
        self.df_pl = pl.from_pandas(df)
        self.df_join_2_pl = pl.from_pandas(df_join_2)
        self.save_output = save_output
        self.operations = {
            'pandas': {
                'gb':   lambda **kwargs: self.gb_operation(pkg = 'pandas', **kwargs),
                'read': lambda **kwargs: self.read_operation(pkg='pandas', **kwargs),
                'join': lambda **kwargs: self.join_operation(pkg='pandas', **kwargs)
            },
            'polars': {
                'gb':   lambda **kwargs: self.gb_operation(pkg = 'polars', **kwargs),
                'read': lambda **kwargs: self.read_operation(pkg='polars', **kwargs),
                'join': lambda **kwargs: self.join_operation(pkg='polars', **kwargs)
            }
        }

    #  Operations -------------------------------------------------------------
        
    def read_operation(self, pkg, path, **kwargs):
        if pkg == 'pandas':
            return pd.read_parquet(path)
        elif pkg == 'polars':
            return pl.read_parquet(path)
        
        
    def gb_operation(self, pkg, with_lambda=False):
        if pkg == 'pandas':
            if with_lambda:
                return (
                    self.df.groupby(['grp', 'grp_3'], as_index=False, sort=False)
                    .agg(
                        x_mean_sq = pd.NamedAgg('x', lambda x: x.mean()**2),
                        y_sum_10  = pd.NamedAgg('y', lambda x: x.sum()/10)
                    )
                )
            else:
                return (
                    self.df
                    .groupby(['grp', 'grp_3'], as_index=False, sort=False)
                    .agg({'x': 'mean', 'y': 'sum'})
                )
        elif pkg == 'polars':
            if with_lambda:
                return (
                    self.df_pl
                    .group_by(['grp', 'grp_3'], maintain_order=True)
                    .agg(
                        x_mean_sq = pl.map_groups('x', lambda x: x[0].mean()**2),
                        y_sum_10  = pl.map_groups('y', lambda x: x[0].sum()/10)
                    )
                )
            else:
                return (
                    self.df_pl
                    .group_by(['grp', 'grp_3'], maintain_order=True)
                    .agg(
                        x_mean = pl.mean('x'),
                        y_sum  = pl.sum('y')
                    )
                )
            
    def join_operation(self, pkg, **kwargs):
        if pkg == 'pandas':
            # return self.df.set_index(['grp', 'grp_2']).join(self.df_join_2.set_index(['grp', 'grp_2']), **kwargs)
            return self.df.merge(self.df_join_2, **kwargs) # merge was faster for this setting
        elif pkg == 'polars':
            return self.df_pl.join(self.df_join_2_pl, **kwargs)
        

    #  Run --------------------------------------------------------------------
            
    def run_operation(self, pkg, type, **kwargs):
        if pkg not in self.operations or type not in self.operations[pkg]:
            raise ValueError("Invalid pkg or type argument.")
        return self.operations[pkg][type](**kwargs)

    def run_comparison(self, type, **kwargs):
        for i in range(self.n_reps):
            for pkg in ['pandas', 'polars']:
                start_time = time.time()
                self.run_operation(pkg, type, **kwargs)
                end_time = time.time()
                self.times[pkg].append((end_time - start_time))

        for pkg in ['pandas', 'polars']:
            times_median = np.median(np.array(self.times[pkg]))
            times_median = times_median * 1000 if self.time_format == 'ms' else times_median
            print(f"{pkg.capitalize()} execution time (median {self.time_format} across {self.n_reps} iterations):", times_median.round(2))

        speedup = [pandas_time / polars_time for pandas_time, polars_time in zip(self.times['pandas'], self.times['polars'])]
        print("Speedup:", np.median(np.array(speedup)).round(5))
        
        if self.save_output:
            result_df = pl.DataFrame(
                {
                    'operation': [type],
                    'n_reps': [self.n_reps],
                    'time_format': [self.time_format],
                    'median_pandas_time': np.median(np.array(self.times['pandas'])),
                    'median_polars_time': np.median(np.array(self.times['polars'])),
                    'median_polars_speedup': np.median(np.array(speedup))
                }
            )
            return result_df


nr = 50

res = [
    TimedComparison(df, n_reps=nr, save_output=True).run_comparison(type = 'gb', with_lambda=False).with_columns(setting = pl.lit('lambda-false')),
    TimedComparison(df, n_reps=nr, time_format='s', save_output=True).run_comparison(type = 'gb', with_lambda=True).with_columns(setting = pl.lit('lambda-true')),
    TimedComparison(df, n_reps=nr, save_output=True).run_comparison(type = 'read', path='data/df.pq').with_columns(setting = pl.lit('NA')),
    TimedComparison(df_join_1, df_join_2, n_reps=nr, save_output=True).run_comparison(type = 'join', on = ['grp', 'grp_2'], how = 'inner').with_columns(setting = pl.lit('inner')),
    TimedComparison(df_join_1, df_join_2, n_reps=nr, save_output=True).run_comparison(type = 'join', on = ['grp', 'grp_2'], how = 'left').with_columns(setting = pl.lit('left')),
    TimedComparison(df_join_2, df_join_1, n_reps=nr, save_output=True).run_comparison(type = 'join', on = ['grp', 'grp_2'], how = 'left').with_columns(setting = pl.lit('right'))
]

pl.concat(res).write_parquet('data/bench_results_py.pq')