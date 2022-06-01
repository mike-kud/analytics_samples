from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph

from airflow.decorators import dag, task

def df_from_ch(query):
    connection = {'host': 'host_url',
                  'database': 'db',
                  'user': 'user',
                  'password': 'password'
                 }

    df = ph.read_clickhouse(query, connection=connection)
    return df

default_args = {
    'owner': 'owner',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 5, 18),
}

schedule_interval = '0 5 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_daily():

    @task()
    def extract_feed():
        q = '''
            SELECT 
                toDate(time) as event_date,
                user_id, 
                countIf(user_id, action='like') as likes, 
                countIf(user_id, action='view') as views,
                gender,
                age,
                os
            FROM 
                simulator_20220420.feed_actions
            WHERE 
                toDate(time) == today() - 1
            GROUP BY 
                event_date,
                user_id, 
                gender,
                age,
                os'''

        df = df_from_ch(q)
        return df

    @task
    def extract_messages():
        q = '''SELECT 
                    user_id, 
                    messages_sent, 
                    users_sent, 
                    messages_received, 
                    users_received
                FROM 
                    (SELECT
                        user_id,
                        toDate(time) as event_date,
                        count(reciever_id) as messages_sent,
                        count (distinct reciever_id) as users_sent
                    FROM 
                        simulator_20220420.message_actions
                    WHERE 
                        event_date == today() - 1
                    GROUP BY
                        event_date,
                        user_id) t1
                LEFT JOIN
                    (SELECT
                        reciever_id,
                        count(user_id) as messages_received,
                        count(distinct user_id) as users_received
                    FROM 
                        simulator_20220420.message_actions
                    WHERE 
                        toDate(time) == today() - 1
                    GROUP BY
                        reciever_id) t2
                ON user_id == reciever_id'''
    
        df = df_from_ch(q)
        return df

    @task
    def join_tables(df1, df2):
        df_joined = df1.merge(df2, how='left', on='user_id').fillna(0)
        return df_joined

    @task
    def transform(df):
            df_transformed = df.groupby(['event_date', 'gender', 'os', 'age'])\
                [['likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']]\
                .sum()\
                .reset_index()
            df_transformed['event_date'] = df_transformed['event_date'].apply(lambda x: datetime.isoformat(x))
            df_transformed = df_transformed.astype({
                        'views':'int', \
                        'likes':'int', \
                        'messages_received':'int', \
                        'messages_sent':'int', \
                        'users_received':'int', \
                        'users_sent':'int'}) 

            return df_transformed

    @task
    def load(df):
        connection = {'host': 'host_url',
                      'database':'database',
                      'user':'user', 
                      'password':'password'
                     }
        
        create = '''CREATE TABLE IF NOT EXISTS db.table_name
                        (
                        event_date datetime,
                        gender TEXT,
                        os TEXT,
                        age INTEGER,
                        views INTEGER,
                        likes INTEGER,
                        messages_received INTEGER,
                        messages_sent INTEGER,
                        users_received INTEGER,
                        users_sent INTEGER
                        ) ENGINE = MergeTree ORDER BY (event_date)'''
        
        ph.execute(create, connection=connection)
        ph.to_clickhouse(df, 'table_name', index=False, connection=connection)

    df_feed = extract_feed()
    df_messages = extract_messages()
    df_joined = join_tables(df_feed, df_messages)
    df_transfromed = transform(df_joined)
    load(df_transfromed)

dag_daily = dag_daily()