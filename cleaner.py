import argparse
import datetime as dt
import pathlib
import os
import pandas as pd
import re

SEP = '-' * 20
MKT_OPEN = dt.time(10, 0)
MKT_CLOSE = dt.time(18, 40)
KNOWN_PERIODS = {1: 1, 2: 2, 5: 5, 10: 10, 15: 15, 30: 30, 60: 60,
                 'daily': None, 'weekly': None, 'Monthly': None}


def check_args(args: argparse.Namespace):
    print(SEP)
    if not args.source_file.is_file():
        raise Exception(f"Filename {args.source_file} doesn\'t exist.")
    print(f'Source file: {args.source_file}')

    if not args.dest_file:
        args.dest_file = args.source_file.parent.joinpath(
            args.source_file.stem + '.clean' + args.source_file.suffix)
    elif not args.dest_file.parent.is_dir():
        raise Exception(f"Destination {args.dest_file}: path "
                        "doesn\'t exist.")

    if not args.check_only:
        print(f'Destination file: {args.dest_file}')
    else:
        print('CHECK ONLY, cleaned data will not be saved.')

    if args.interactive and input('Continue[Y/n]? ').lower() == 'n':
        exit()
    else:
        print(SEP)


def load_source_file(args: argparse.Namespace) -> pd.DataFrame:
    print('Loading source file... ', end='')
    try:
        df = pd.read_csv(args.source_file, dtype={'date': str, 'time': str})
        print(f'OK\n{SEP}')
    except Exception as err:
        print(f'ERROR\n{SEP}')
        raise err
    return df


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


def get_full_list_of_daily_bar_times(start_time: dt.time,
                                     end_time: dt.time,
                                     period_min: int,
                                     skip_daily_clearing: bool = False):
    """Returns a list of datetime.time objects from first till last bar time
    (included) considering daily 14:00-14:05 clearing break for futures"""

    assert start_time <= end_time

    var_dt = dt.datetime.combine(dt.date.today(), start_time)
    full_list_of_daily_bar_times = []

    while var_dt.time() < end_time:
        if (skip_daily_clearing or var_dt.hour != 14
                or var_dt.minute not in [0, 1, 2, 3, 4]):
            full_list_of_daily_bar_times.append(var_dt.time())
        var_dt += dt.timedelta(minutes=period_min)

    return full_list_of_daily_bar_times


def intraday_data_check(df: pd.DataFrame, period_min: int):
    """Checks intraday data for given minute timeframe 'period_min'"""

    # Complete list of daily bar times for given period
    full_times_set = set(get_full_list_of_daily_bar_times(
        MKT_OPEN, MKT_CLOSE, period_min, args.stock))
    print(f'Total bars per day: {len(full_times_set)}')
    warning = None

    for date in sorted(df['datetime'].dt.date.unique()):
        times_set = set(
            df[df['datetime'].dt.date == date]['datetime'].dt.time
        )
        missing_bars_set = full_times_set - times_set
        irregular_bars_set = times_set - full_times_set

        if len(missing_bars_set) or len(irregular_bars_set):
            warning = f'{date} | WARNING: '
            if len(missing_bars_set):
                warning += (
                    f'missing {len(missing_bars_set)} of '
                    f'{len(full_times_set)} bars, ['
                    f'{sorted(list(missing_bars_set))[0]} : '
                    f'{sorted(list(missing_bars_set))[-1]}] '
                )
            if len(irregular_bars_set):
                warning += (
                    'Irregular bars: '
                    f'{len(times_set - full_times_set)}, last '
                    f'{sorted(list(times_set - full_times_set))[-1]}. '
                )
            print(warning)

    if not warning:
        print('No missing/irregular bars - OK')


def process_dataframe(df: pd.DataFrame, args: argparse.Namespace
                      ) -> pd.DataFrame:

    # Converting colum names: removing '<' and '>' and coercing to lower case
    df.columns = list(map(lambda x: re.sub(r'[<>]', '', x).lower(),
                          df.columns))

    # Checking if period is unique
    if not len(df['per'].unique()) == 1:
        raise Exception(f"Period data is not unique: {df['per'].unique()}")
    period = df['per'].iloc[0]

    # Converting ticker: removing 'SPBFUT', 'SPFB'
    print(f'Renaming ticker(s) from {df["ticker"].unique()} to ', end='')
    df['ticker'] = df['ticker'].apply(lambda x:
                                      re.sub(r'.?SPBFUT.?|.?SPFB.?', '', x)
                                      .strip())
    print(f'{df["ticker"].unique()}')

    # Creating datetime column
    if {'date', 'time'} <= set(df.columns):
        print("Creating \'datetime\' column")
        df['datetime'] = (
            (df['date'].astype(str) + ' '
             + df['time'].astype(str).apply(
                 lambda x: '0' + x if len(x) == 5 else x)
             )
            .apply(dt.datetime.fromisoformat)
        )
    elif 'datetime' in set(df.columns):
        print("[INFO]: \'datetime\' column already exists, converting to "
              "datetime dtype.")
        df['datetime'] = pd.to_datetime(df['datetime'], errors='raise')
    else:
        raise Exception(f"Neither {'date', 'time'} nor {'datetime'}"
                        'in source data.')

    if ('vol' in set(df.columns)
            and not pd.api.types.is_integer_dtype(df['vol'].dtype)):
        print("Casting 'vol' to integer dtype")
        df['vol'] = df['vol'].astype(int)

    # Dropping date and time columns
    if not args.keep_date_time and {'date', 'time'} <= set(df.columns):
        print("Dropping \'date\' and \'time\' columns")
        df = df.drop(['date', 'time'], axis='columns')

    # Dropping all data out of optional start and end dates
    if args.start_date:
        print(f'Dropping data before {args.start_date}.')
        df = df[df['datetime'].dt.date >= args.start_date]
    if args.end_date:
        print(f'Dropping data after {args.end_date}')
        df = df[df['datetime'].dt.date <= args.end_date]

    # Dropping extended hours
    print(f'Keep extended hours = {args.extended_hours}, ', end='')
    if args.extended_hours:
        print('skipping.')
    else:
        print('deleting (if any).')
        df = df[(df['datetime'].dt.time >= MKT_OPEN)
                & (df['datetime'].dt.time < MKT_CLOSE)]

    # Checking for duplicates
    if df['datetime'].duplicated().sum():
        print("[WARNING]: \'datetime\' column has duplicates, dropping.")
        df = df.drop_duplicates('datetime')
    else:
        print('No datetime duplicates found')

    # Sorting by datetime
    df = df.sort_values('datetime')

    # Print info on dates and period
    print(f"Dates from {df['datetime'].dt.date.min()} till "
          f"{df['datetime'].dt.date.max()}. "
          f'Period: {period} (known={period in KNOWN_PERIODS})')

    if args.interactive and input('Continue[Y/n]? ').lower() == 'n':
        exit()
    else:
        print(SEP)

    # Check indraday data (only for minute-like periods)
    if KNOWN_PERIODS.get(period):
        intraday_data_check(df, KNOWN_PERIODS[period])
        print(SEP)

    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Processes raw QUIK .csv bar history file, "
        + "checks and cleans data, writes 'clean' output file")
    parser.add_argument("source_file", type=pathlib.Path,
                        help="Source file to process")
    parser.add_argument("-d", "--dest-file", default=None, type=pathlib.Path,
                        help="output file")
    parser.add_argument("-s", "--stock", action='store_true',
                        help="disregard  MOEX futures clearing break "
                        "14:00-14:05")
    parser.add_argument('-e', '--extended-hours', action='store_true',
                        help='keep extended trading hours (deleted '
                        'otherwise)')
    parser.add_argument('-c', '--check-only', action='store_true',
                        help='do not save proceessed file')
    parser.add_argument('-k', '--keep-date-time', action='store_true',
                        help='do not drop original date and time columns')
    parser.add_argument('--start-date', type=dt.date.fromisoformat,
                        help='drop all data before the start date')
    parser.add_argument('--end-date', type=dt.date.fromisoformat,
                        help='drop all data after the end date')
    parser.add_argument('-i', '--interactive', action='store_true',
                        help='give interactive prompts')

    args = parser.parse_args()

    try:
        check_args(args)
        df = load_source_file(args)
        df = process_dataframe(df, args)
        save_to_dest_file(df, args)

    except Exception as error:
        print(f'{os.path.basename(__file__)}: {error.__class__.__name__}: '
              f'{error}')
        exit(1)
