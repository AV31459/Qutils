import argparse
import datetime as dt
import pathlib
import os
import pandas as pd


SEP = '-' * 20


def check_args(args: argparse.Namespace):
    print(SEP)
    # Check if file_1 and file_2 exist
    if not args.file_1.is_file():
        raise Exception(f"Filename {args.file_1} doesn\'t exist.")
    print(f'File 1: {args.file_1}')

    if not args.file_2.is_file():
        raise Exception(f"Filename {args.file_2} doesn\'t exist.")
    print(f'File 2: {args.file_2}')

    # Check if dest_file is a valid filename
    if args.check_only:
        print('CHECK ONLY, merged data will not be saved.')
    elif not args.dest_file.parent.is_dir():
        raise Exception(f"Destination {args.dest_file}: path "
                        "doesn\'t exist.")
    else:
        print(f'Destination file: {args.dest_file}')

    if args.interactive and input('Continue[Y/n]? ').lower() == 'n':
        exit()
    else:
        print(SEP)


def load_source_files(args: argparse.Namespace) -> tuple[pd.DataFrame,
                                                         pd.DataFrame]:
    print('Loading source files... ', end='')
    try:
        df1 = pd.read_csv(args.file_1, parse_dates=['datetime'])
        df2 = pd.read_csv(args.file_2, parse_dates=['datetime'])
        print(f'OK\n{SEP}')
    except Exception as err:
        print(f'ERROR\n{SEP}')
        raise err
    return df1, df2


def save_to_dest_file(df: pd.DataFrame, args: argparse.Namespace):
    if args.check_only:
        return
    print('Exporting to destination file... ', end='')
    try:
        df.to_csv(args.dest_file, index=False)
        print(f'OK\n{SEP}')
    except Exception as err:
        print(f'ERROR\n{SEP}')
        raise err


def check_source_data(source_dfs: tuple[pd.DataFrame, pd.DataFrame]):

    df1_dtypes = pd.Series()

    for i, df in enumerate(source_dfs):
        if 'datetime' not in df.columns:
            raise Exception(f"No 'datetime' column in file_{i + 1}")
        if 'vol' not in df.columns:
            raise Exception(f"No volume ('vol') column in file_{i + 1}")
        if not pd.api.types.is_integer_dtype(df['vol'].dtype):
            raise Exception(f"Volume ('vol') column in file_{i + 1} "
                            "is not integer")

        if i == 0:
            df1_dtypes = df.dtypes.sort_index()
        elif set(df.columns) != set(df1_dtypes.index):
            raise Exception('Source files have different columns '
                            f'{set(df.columns) ^ set(df1_dtypes.index)}')
        elif df1_dtypes.compare(df.dtypes.sort_index()).size:
            raise Exception('Files have different data types: '
                            f'{df1_dtypes.compare(df.dtypes.sort_index())}')


def merge_data(df1: pd.DataFrame, df2: pd.DataFrame,
               args: argparse.Namespace) -> pd.DataFrame:

    for i, df in enumerate([df1, df2]):
        unique_dates = df['datetime'].dt.date.unique()
        print(f"File{i + 1}: ticker(s) {df['ticker'].unique()}, "
              f'days {unique_dates.size}: from {unique_dates.min()} '
              f'till {unique_dates.max()}')

    # Dropping data before and after given optional dates
    if args.start_date:
        print(f'Dropping data before {args.start_date}')
        df1 = df1[df1['datetime'].dt.date >= args.start_date]
        df2 = df2[df2['datetime'].dt.date >= args.start_date]
    if args.end_date:
        print(f'Dropping data after {args.end_date}')
        df1 = df1[df1['datetime'].dt.date <= args.end_date]
        df2 = df2[df2['datetime'].dt.date <= args.end_date]

    # Calculating daily volumes
    df1_vol = df1.groupby(df1['datetime'].dt.date)['vol'].sum().sort_index()
    df2_vol = df2.groupby(df2['datetime'].dt.date)['vol'].sum().sort_index()

    print('Merging data...', end='')

    # Creating two sets of dates, corresponding to data from df1 or df2
    df1_dates = (
        (set(df1_vol.index) - set(df2_vol.index))
        | set((df1_vol - df2_vol)[(df1_vol - df2_vol) >= 0].index)
    )
    df2_dates = set(df2_vol.index) - df1_dates

    # Naive data concatenation by date
    new_df = pd.concat([
        df1[df1['datetime'].dt.date.apply(lambda x: True if x in df1_dates
                                          else False)],
        df2[df2['datetime'].dt.date.apply(lambda x: True if x in df2_dates
                                          else False)]],
        ignore_index=True
    ).sort_values('datetime')

    print('OK!')

    unique_dates = new_df['datetime'].dt.date.unique()
    print(f"Merged: ticker(s) {new_df['ticker'].unique()}, "
          f'days {unique_dates.size}: from {unique_dates.min()} '
          f'till {unique_dates.max()}')

    if (args.interactive
            and input('Print tickers per day [y/N]? ').lower() == 'y'):
        new_df_tickers = (new_df.groupby(new_df['datetime'].dt.date)['ticker']
                          .max())
        for date in (new_df_tickers.index):
            print(f'{date} : {new_df_tickers[date]}')

    print(SEP)

    return new_df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Merges two processed (i.e. cleaned) QUICK csv files date "
        "by date. If both files contain same date (futures overlapping), "
        "the one with larger daily volumes is taken."
    )
    parser.add_argument("file_1", type=pathlib.Path, help="First file")
    parser.add_argument("file_2", type=pathlib.Path, help="Second file")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-d", "--dest-file", default=None, type=pathlib.Path,
                       help="output file")
    group.add_argument('-c', '--check-only', action='store_true',
                       help='do not save proceessed file')
    parser.add_argument('--start-date', type=dt.date.fromisoformat,
                        help='drop all data before the start date')
    parser.add_argument('--end-date', type=dt.date.fromisoformat,
                        help='drop all data after the end date')
    parser.add_argument('-i', '--interactive', action='store_true',
                        help='give interactive prompts')

    args = parser.parse_args()

    try:
        check_args(args)
        df1, df2 = load_source_files(args)
        check_source_data((df1, df2))
        dest_df = merge_data(df1, df2, args)
        save_to_dest_file(dest_df, args)

    except Exception as error:
        print(f'{os.path.basename(__file__)}: {error.__class__.__name__}: '
              f'{error}')
        exit(1)
