import pandas as pd

def re_partition(load_dt):
    base_path='~/movie_data/data/extract'
    save_path='~/data/movie/repartition'

    df = pd.read_parquet(f'{base_path}/load_dt={load_dt}')
    df['load_dt'] = load_dt
    df.to_parquet(save_path, partition_cols=['load_dt','multiMovieYn', 'repNationCd'])
