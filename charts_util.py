from collections import defaultdict

from pyecharts import options as opts
from pyecharts.charts import WordCloud, Line
from pyecharts.globals import SymbolType
import pandas as pd
import logging
from time import localtime
from csv_util import CsvUtil

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

OUTFILE = f"charts/{localtime().tm_year}" \
          f"-{localtime().tm_mon}" \
          f"-{localtime().tm_mday}" \
          f"--{localtime().tm_hour}h" \
          f"-{localtime().tm_min}m" \
          f"-{localtime().tm_sec}s.out"


class ChartsUtil:
    @staticmethod
    def author_wordcloud(data: pd.DataFrame):
        try:
            author_counts = data.author.value_counts().to_dict()
            WordCloud().add(series_name="作者词云图", data_pair=[(i, author_counts[i]) for i in author_counts],
                            word_size_range=[20, 100],
                            shape=SymbolType.RECT).set_global_opts(
                title_opts=opts.TitleOpts(title="作者词云图")).render(OUTFILE + '.author_wordcloud.html')
            logger.info(f"Add {len(author_counts)} authors to wordcloud...")
        except IndexError:
            logger.error(f"There is not index of author!")

    @staticmethod
    def tag_wordcloud(data: pd.DataFrame):
        try:
            tag_counts = defaultdict(int)
            for t in data.tag.values:
                for each_tag in t.strip().split('-'):
                    tag_counts[each_tag] += 1
            WordCloud().add(series_name="作品类型词云图", data_pair=[(i, tag_counts[i]) for i in tag_counts],
                            word_size_range=[20, 100],
                            shape=SymbolType.RECT).set_global_opts(
                title_opts=opts.TitleOpts(title="作品类型词云图")).render(OUTFILE + '.tags_wordcloud.html')
            logger.info(f"Add {len(tag_counts)} tags to wordcloud...")
        except IndexError:
            logger.error(f"There is not index of tag!")

    @staticmethod
    def score_line(data: pd.DataFrame):
        try:
            title = list(data.name)
            score = list(data.score)
            Line().add_xaxis(xaxis_data=title).add_yaxis("作品积分", score, symbol="emptyCircle", is_symbol_show=True,
                                                         label_opts=opts.LabelOpts(is_show=True)).set_global_opts(
                title_opts=opts.TitleOpts(title="作品积分折线图", pos_left="center"),
                tooltip_opts=opts.TooltipOpts(trigger='axis'), datazoom_opts=[
                    opts.DataZoomOpts(
                        is_show=True,
                        is_realtime=True,
                        range_start=0,
                        range_end=5,
                        xaxis_index=[0, 1],
                    )
                ], toolbox_opts=opts.ToolboxOpts(
                    is_show=True,
                    feature={
                        "dataZoom": {"yAxisIndex": "none"},
                        "restore": {},
                        "saveAsImage": {},
                    },
                )).render(
                OUTFILE + '.score_line.html')
            logger.info(f"Add {len(title)} novels to line...")
        except IndexError:
            logger.error(f"There is not index of score!")


if __name__ == '__main__':
    charts_util = ChartsUtil()
    csv_util = CsvUtil()
    dt = csv_util.read_file('out/bookshelf_3500_sortbycollect/2023-3-13--17h-2m-27s.out.csv')
    # charts_util.author_wordcloud(dt)
    # charts_util.tag_wordcloud(dt)
    charts_util.score_line(dt)
