import pandas as pd
from pygg import *

lq_micro = pd.read_csv('lineage_ops_4_9_2023_with_rand_and_skew.csv')
data = lq_micro

print(data)

ops = {
    'groupby',
    'filter',
    'perfgroupby',
    'hashjoin',
    'mergejoin',
    'nljoin',
    'simpleagg',
    'orderby',
}

data = data[data['avg_parse_time'] != 0]
data = data[data['oids'] == 1000]
data['avg_duration'] = data['avg_duration'] - data['avg_parse_time'] # Subtract out parse time
data = data[['oids', 'avg_duration', 'op']]
data = data[data['op'].isin(ops)]

data['avg_duration'] = data['avg_duration'].mul(1000)
data = data.rename(columns={'oids': 'Queried_ID_Count', 'avg_duration': 'Runtime'})

legend = theme_bw() + theme(**{
    "legend.background": element_blank(),
    "legend.key" : element_blank(),
    "legend.title":element_blank(),
    "text": element_text(colour = "'#333333'", size=8, family = "'Arial'"),
    "axis.text": element_text(colour = "'#333333'", size=8),
    "strip.text": element_text(color=esc("#333333")),
    "legend.margin": margin(t = 0, r = 0, b = 0, l = 0, unit = esc("pt")),
    "legend.text": element_text(colour = "'#333333'", size=8, family = "'Arial'"),
    "legend.key.size": unit(6, esc('pt')),
    "axis.text.y": element_blank(),
    "axis.ticks.y": element_blank(),
    "axis.title.y": element_blank(),
})

order_map = {
    'simpleagg': '7simpleagg',
    'orderby': '6orderby',
    'filter': '5filter',
    'groupby': '4groupby',
    'nljoin': '3nljoin',
    'perfgroupby': '2perfgroupby',
    'mergejoin': '1mergejoin',
    'hashjoin': '0hashjoin',
}

data['orderedOp'] = data.apply(
    lambda row: order_map[row['op']],
    axis=1,
)

condition_labels = [esc('Simple Aggregate'), esc('Order By'), esc('Filter'), esc('Group By'), esc('Nested Loop Join'), esc('Perfect Group By'), esc('Merge Join'), esc('Hash Join')]
condition_labels.reverse()

p = ggplot(data, aes(x='Runtime', y='orderedOp', condition='orderedOp', color='orderedOp', fill='orderedOp', group='orderedOp')) \
    + scale_x_continuous(breaks=[0.0, 0.5, 1.0, 1.5, 2.0], labels=[esc('0.0ms'), esc('0.5ms'), esc('1ms'), esc('1.5ms'), esc('2ms')]) \
    + geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5) \
    + scale_y_discrete(breaks=condition_labels) \
    + scale_color_discrete(labels=condition_labels) \
    + scale_fill_discrete(labels=condition_labels) \
    + guides(colour = guide_legend(reverse=True), fill = guide_legend(reverse=True)) \
    + legend
ggsave("lq_microbench.png", p, width=3, height=1.5)
