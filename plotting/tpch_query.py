import pandas as pd
from pygg import *
import duckdb

smokedduck = pd.read_csv('tpch_sf10_2_28_2023_2.csv')
logical_rid = pd.read_csv('tpch_bw_notes_oct_4_2023_sf10_lineage_type_Logical-RID.csv')

smokedduck = smokedduck[['queryId', 'avg_duration']]
smokedduck['system'] = ['SD_Query' for _ in range(len(smokedduck))]
smokedduck = smokedduck.rename(columns={'queryId': 'query', 'avg_duration': 'runtime'})
logical_rid = logical_rid[['query', 'runtime']]
logical_rid['system'] = ['Logical-OPT' for _ in range(len(logical_rid))]
data = smokedduck.append(logical_rid)
data['query'] = data['query'].astype('str')
data['runtime'] = data['runtime'].mul(1000)
data = data.rename(columns={'query': 'Query', 'runtime': 'Runtime'})

# Source Sans Pro Light
legend = theme_bw() + theme(**{
    "legend.background": element_blank(),
    "legend.justification":"c(1,0)", "legend.position":"c(1,0)",
    "legend.key" : element_blank(),
    "legend.title":element_blank(),
    "text": element_text(colour = "'#333333'", size=11, family = "'Arial'"),
    "axis.text": element_text(colour = "'#333333'", size=11),
    "panel.border": element_rect(color=esc("#e0e0e0")),
    "strip.text": element_text(color=esc("#333333"))
})
legend_bottom = legend + theme(**{
    "legend.position":esc("bottom"),
})

legend_side = legend + theme(**{
    "legend.position":esc("right"),
    "legend.margin":"margin(t = 0, unit='cm')"
})



duckdb.register('data', data)
data = duckdb.sql("""
    select d1.Runtime as sd, d2.Runtime as logical, d1.Query::text as Query
    from data d1, data d2
    where d1.Query = d2.Query and d1.system <> d2.system
    and d1.system = 'SD_Query'
""").fetchdf()
print(data)

p = ggplot(data, aes(x='Query', ymin='sd', ymax='logical'))
p += geom_linerange(color=esc("gray"))
p += geom_point(aes(y='sd', color=esc('SD'), shape=esc("SD")))
p += geom_point(aes(y='logical', color=esc('Logical'), shape=esc("Logical")))
p += scale_colour_discrete(name=esc("System"))
p += scale_shape_discrete(name=esc("System"))
p += axis_labels("TPC-H Query", "Runtime (log)", "continuous", "log10", ykwargs=dict(breaks=[1, 10, 100, 1000, 10000], labels=[esc('1ms'), esc('10ms'), esc('100ms'), esc('1sec'), esc('10sec')]))
p += legend_side
ggsave("querying_tpch_sf10.png", p, width=4, height=1.5)