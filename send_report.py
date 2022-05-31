import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io
import telegram
from CH import Getch
import os

sns.set()

bot = telegram.Bot(token=os.environ.get("REPORT_BOT_TOKEN"))

def create_plot(data, value='value', title='title', ax=None, hue=None):
    sns.lineplot(data=data, x=data.iloc[:, 0], y=data.iloc[:, -1], marker='o', ax=ax, hue=hue)
        
    ax.set_title(title, fontsize=20, pad=10)
    ax.set_ylabel(value)
    ax.set_xlabel('')

def create_send_plot(chat_id=671530):
    fig, axes = plt.subplots(2,2, figsize=(20, 15))

    fig.suptitle('Дополнительные метрики за 7 дней', fontsize=30, y=0.96)
    create_plot(data=users_by_source, value='Пользователи', title='Новые пользователи по источникам', hue='source', ax=axes[0,0])
    create_plot(data=users_by_actions_melted, value='Пользователи', title=' ', ax=axes[0,1], hue='action')
    create_plot(data=new_posts, value='Посты', title='Количество новых постов', ax=axes[1,0])
    create_plot(data=actions_per_users_melted, value='Действия на пользователя', title='Количество действий на пользователя', ax=axes[1,1], hue='action')
    
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = f'%metric_plot.png'
    plot_object.seek(0)
    
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

def get_percent(x, y):
    percent = round((x/ y - 1) * 100, 2)
    if percent > 0:
        return f'+{percent}%'
    else:
        return f'{percent}%'

def send_message(message='', chat_id=671530):
    bot.sendMessage(chat_id=chat_id, text=message)

def create_plot(data, value='value', title='title', ax=None, hue=None):
    sns.lineplot(data=data, x=data.iloc[:, 0], y=data.iloc[:, -1], marker='o', ax=ax, hue=hue)
        
    ax.set_title(title, fontsize=20, pad=10)
    ax.set_ylabel(value)
    ax.set_xlabel('')

users_by_source = Getch('''select toStartOfDay(toDateTime(first_entry)) as day, 
                                  source,
                                  count(distinct user_id) as users
                            from
                                (select user_id,
                                        source,
                                        min(toDate(time)) as first_entry
                                 from simulator_20220420.feed_actions
                                 group by user_id,
                                          source) as new_users
                            where day >= today() - 7 and day < today()
                            group by source, day
                            order by day''').df

new_posts = Getch('''select toStartOfDay(toDateTime(post_time)) as day,
                            count(post_id) as posts
                    from 
                        (select post_id,
                                min(time) as post_time
                        from simulator_20220420.feed_actions
                        group by post_id) as upst
                    where day >= today() - 7 and day < today()
                    group by day
                    order by day''').df

users_by_actions = Getch('''select day,
                                countIf(user_id, views > 0) as view,
                                countIf(user_id, likes > 0) as like,
                                countIf(user_id, messages > 0) as send_message
                            from
                            (select day,
                                    user_id,
                                    user_feed_act.views,
                                    user_feed_act.likes,
                                    user_messages.messages
                            from
                                (select toStartOfDay(time) as day,
                                        user_id,
                                        countIf(user_id, action = 'view') as views,
                                        countIf(user_id, action = 'like') as likes
                                from simulator_20220420.feed_actions
                                group by day, user_id) as user_feed_act
                            full outer join
                                (select toStartOfDay(time) as day,
                                        user_id,
                                        count(user_id) as messages
                                from simulator_20220420.message_actions
                                group by day, user_id) as user_messages 
                                using (day, user_id)
                            )
                            where day >= today() - 7 and day < today()
                            group by day
                            order by day
                        ''').df

actions_per_users = Getch('''select day,
                                    sum(views) / count(DISTINCT(user_id)) as views_per_user,
                                    sum(likes) / count(DISTINCT(user_id)) as likes_per_user,
                                    sum(messages) / count(DISTINCT(user_id)) as messages_per_user
                                from
                                (select day,
                                        user_id,
                                        user_feed_act.views,
                                        user_feed_act.likes,
                                        user_messages.messages
                                from
                                    (select toStartOfDay(time) as day,
                                            user_id,
                                            countIf(user_id, action = 'view') as views,
                                            countIf(user_id, action = 'like') as likes
                                    from simulator_20220420.feed_actions
                                    group by day, user_id) as user_feed_act
                                full outer join
                                    (select toStartOfDay(time) as day,
                                            user_id,
                                            count(user_id) as messages
                                    from simulator_20220420.message_actions
                                    group by day, user_id
                                    order by user_id, day) as user_messages 
                                            using (day, user_id)
                                    )
                                where day >= today() - 7 and day < today()
                                group by day
                                order by day''').df

users_by_actions_melted = pd.melt(users_by_actions, id_vars='day', var_name='action', value_name='users')
actions_per_users_melted = pd.melt(actions_per_users, id_vars='day', var_name='action', value_name='rate')

report_date = users_by_source.iloc[-1,0].strftime('%-d/%-m/%Y')

message = f'''Расширенный отчет за {report_date}
Пользователи пришедшие с рекламы: {users_by_source.iloc[-2,2]} ({get_percent(users_by_source.iloc[-2,2], users_by_source.iloc[-4,2])})
Пользователи с органики: {users_by_source.iloc[-1,2]} ({get_percent(users_by_source.iloc[-1,2], users_by_source.iloc[-3,2])})
Новые посты: {new_posts.iloc[-1,1]} ({get_percent(new_posts.iloc[-1,1], new_posts.iloc[-2,1])})
Количество пользователей которые
- смотрят посты: {users_by_actions.iloc[-1,1]} ({get_percent(users_by_actions.iloc[-1,1], users_by_actions.iloc[-2,1])})
- ставят лайки: {users_by_actions.iloc[-1,2]} ({get_percent(users_by_actions.iloc[-1,2], users_by_actions.iloc[-2,2])})
- пересылают сообщения: {users_by_actions.iloc[-1,3]} ({get_percent(users_by_actions.iloc[-1,3], users_by_actions.iloc[-2,3])})
Количество действий на одного пользователя
- просмотр: {round(actions_per_users.iloc[-1,1], 2)} ({get_percent(actions_per_users.iloc[-1,1], actions_per_users.iloc[-2,1])})
- лайк: {round(actions_per_users.iloc[-1,2], 2)} ({get_percent(actions_per_users.iloc[-1,2], actions_per_users.iloc[-2,2])})
- сообщение: {round(actions_per_users.iloc[-1,3], 2)} ({get_percent(actions_per_users.iloc[-1,3], actions_per_users.iloc[-2,3])})
'''

send_message(message=message, chat_id=-123456789)
create_send_plot(chat_id=-123456789)


