import pandas as pd
import logging
from time import localtime

logger = logging.Logger("main")
handler = logging.FileHandler(f"logs/"
                              f"{localtime().tm_year}-"
                              f"{localtime().tm_mon}-"
                              f"{localtime().tm_mday}--"
                              f"{localtime().tm_hour}h-"
                              f"{localtime().tm_min}m-"
                              f"{localtime().tm_sec}s.log",
                              encoding="utf-8")
formatter = logging.Formatter("[%(levelname)-5.5s][%(funcName)-7.7s][%(lineno)3.3d行]-%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)

logger.addHandler(handler)
logger.addHandler(console)


def read_file(path: str) -> pd.DataFrame:
    logger.info(f"Try to read {path}")
    try:
        data = pd.read_csv(path)
        return data
    except IOError:
        logger.error("Cannot read csv, please check config")
        return None


def remove_duplicate(data: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Try to remove duplicate lines...")
    logger.info(f"Lines count:{len(data)}")
    dup = data.duplicated(subset=['name'])
    logger.info(f"Duplicate lines count:{dup.value_counts()[True]}")
    data = data[~dup]
    logger.info(f"After lines count:{len(data)}")
    return data


def drop_useless_columns(data: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Try to remove useless columns...")
    print(data.head(5))
    data.drop(columns=['author', 'name', 'tag', 'style', 'status', 'pub_time'], inplace=True)
    print("After:")
    print(data.head(5))
    return data


def concat(d1: pd.DataFrame, d2: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Data1 has {len(d1)} lines, Data2 has {len(d2)} lines")
    data = pd.concat([d1, d2], ignore_index=True)
    logger.info(f"After concat, Data has {len(data)} lines")
    return data


def minmax_normalize(data: pd.DataFrame) -> pd.DataFrame:
    st = data[:]
    st = (st - st.min()) / (st.max() - st.min())
    return st


def write_csv(data: pd.DataFrame, path: str) -> str:
    logger.info(f"Try to write csv to {path}...")
    try:
        data.to_csv(path, encoding='utf8', index=False)
        return "Success"
    except IOError:
        logger.error("Cannot write csv, please check config")
        return "Failed"


if __name__ == '__main__':
    # concat('out/p1.csv', 'out/p2.csv')
    # remove_duplicate('out/2023-3-7--20h-46m-36s.out.csv')
    # data1 = read_file('out/2023-3-7--20h-46m-36s.out.csv')
    # data2 = read_file('out/2023-3-6--14h-47m-55s.out.csv')
    # rem = drop_useless_columns(remove_duplicate(concat(data1, data2)))
    # print(minmax_normalize(rem))
    dt = read_file('out/test.csv')
    dt = remove_duplicate(dt)
    write_csv(dt, 'out/test.csv')
