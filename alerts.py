import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import io
import os
from read_db.CH import Getch
from scipy.stats import iqr
from statsmodels.tsa.seasonal import seasonal_decompose

def is_outlier(observation, predictions):
    #проверяем на попадание в границы предсказанных значений
    predicted_observation = predictions[predictions['hm'] == observation['hm'].iloc[0]]
    current_value = observation.iloc[:,3].iloc[0]
    lower_limit = predicted_observation['pred_low'].iloc[0]
    upper_limit = predicted_observation['pred_high'].iloc[0]

    if lower_limit <= current_value <= upper_limit:
        return False
    else:
        return True

def percent_diff(x, y):
    #получаем строчное значение процентной разницы со знаком
    percent = round((x / y - 1) * 100, 2)
    if percent > 0:
        return f'+{percent}%'
    else:
        return f'{percent}%'

def check_anomaly(data, metric):
    #получаем предсказания на текущий день и сравниваем с текущим значением
    #если значение выходит за границы предсказанного, выдаем алерт
    #и возвращаем текущее значени, разницу с предсказанным и датафрейм с предсказанными значениями
    yesterday = (pd.Timestamp.today() - pd.offsets.Day(1)).strftime('%Y-%m-%d')
    today = pd.Timestamp.today().strftime('%Y-%m-%d')

    decomposition = seasonal_decompose(data[metric], #разбираем наши данные на сезонные и трендовые состовляющие
                                   period=96, model='multiplicative') 

    data['detrended'] = decomposition.seasonal #добавляем в датафрейм метрику без тренда
    data['trend'] = decomposition.trend #добавляем отдельно тренд

    predictions = data.groupby('hm')['detrended'].median().to_frame().reset_index() #группируем по пятнадцатиминуткам получаем среднее значение
    predictions['prediction'] = predictions['detrended'] * data[data['date'] == today]['trend'].mean() #предсказываем значения метрики для требуемого 
    predictions['iqr'] = iqr(decomposition.resid.dropna(), rng=[10,90]) * data[data['date'] == yesterday]['trend'].mean() #допустимая погрешность в одну сторону
    predictions['pred_high'] = predictions['prediction'] + predictions['iqr'] #верхняя граница допустимого интервала
    predictions['pred_low'] = predictions['prediction'] - predictions['iqr'] #нижняя граница допустимого интервала

    current_observation = data[-1:]
    predicted_observation = predictions[predictions['hm'] == current_observation['hm'].iloc[0]]
    current_value = current_observation[metric].iloc[0]
    predicted_value = predicted_observation['prediction'].iloc[0]
    diff = percent_diff(current_value, predicted_value)

    if is_outlier(data[-1:], predictions) and is_outlier(data[-2:], predictions) == False: #не выдаем алерт, если выдавали на предыдущем шаге
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, current_value, diff, predictions

def create_plot(data, predictions, metric):
    today = pd.Timestamp.today().strftime('%Y-%m-%d')

    sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
    plt.tight_layout()

    ax = sns.lineplot(data=predictions, x=predictions['hm'], y='prediction') #линия предсказанных значений
    sns.lineplot(data=data[data['date'] == today], x='hm', y=data.columns[3]) #значения метрики для текущего дня
    ax.fill_between(x=predictions['hm'], y1=predictions.pred_low, y2=predictions.pred_high, alpha=0.3) #заполняем по верхним и нижним границам

    for ind, label in enumerate(ax.get_xticklabels()): # этот цикл нужен чтобы разрядить подписи координат по оси Х,
                if ind % 15 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)

    ax.set(xlabel='time') # задаем имя оси Х
    ax.set(ylabel=metric) # задаем имя оси У
    ax.set_title(f'{metric}', fontsize=20)

    #создаем объект графика для отправки ботом
    plot_object = io.BytesIO()
    ax.figure.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = f'{metric}.png'
    plt.close()

    return plot_object

def create_messege(metric, current_value, diff):
    #создаем значения для отправки ботом
    message = f'''Метрика {metric}:\nТекущее значение: {current_value}\nОтклонение от прогноза {diff}'''

    return message

def bot_send(msg, plot, chat_id=-123456789):
    #отправляем сообщение и график в чат
    bot = telegram.Bot(token=os.environ.get("REPORT_BOT_TOKEN"))
    bot.sendMessage(chat_id=chat_id, text=msg)
    bot.sendPhoto(chat_id=chat_id, photo=plot)

def run_check(data, metric):
    #запуск проверки метрик
    is_alert, current_value, diff, predictions = check_anomaly(data, metric)
    
    if is_alert:
       bot_send(create_messege(metric, current_value, diff), create_plot(data, predictions, metric))

feed_metrics_data = Getch('''SELECT
                    toStartOfFifteenMinutes(time) as ts,
                    toDate(ts) as date,
                    formatDateTime(ts, '%R') as hm,
                    uniqExact(user_id) as users,
                    countIf(user_id, action='view') as views,
                    countIf(user_id, action='like') as likes,
                    likes / views * 100 as ctr
                FROM simulator_20220420.feed_actions
                WHERE ts >= today() - 30 and ts < toStartOfFifteenMinutes(now())
                GROUP BY ts, date, hm
                ORDER BY ts''').df

    
for metric in ['users', 'views', 'likes', 'ctr']:
    run_check(feed_metrics_data, metric)

