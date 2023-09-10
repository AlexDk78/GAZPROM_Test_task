import pandas as pd

df = pd.read_excel('/Users/aleksejborovoi/Desktop/Python_projects/Gazprom/flat_table.xlsx') # Здесь обязательно указать ПОЛНЫЙ путь до исходного файла согласно примеру
# Пример для Windows: df = pd.read_excel(r'C:\Users\Ron\Desktop\products.xlsx')
# Пример для MacOS: df = pd.read_excel('/Users/Ron/Desktop/products.xlsx')

df.drop(df[df['gas_type'] ==
        'Транспортировка до Балансового пункта'].index, inplace=True)
df.replace(to_replace='«', value='"', inplace=True, regex=True)
df.replace(to_replace='»', value='"', inplace=True, regex=True)

pattern = r'ООО "Газпром трансгаз ([А-Яа-я\s-]+)"'
df['city'] = df['tg_owner'].str.extract(pattern, expand=False)
cities = set(df['city'].tolist())

for city in cities:
    df.loc[df['tg_from'].str.contains(
        '"ГТ '+city), 'tg_from'] = 'ООО "Газпром трансгаз '+city+'"'
    
df.loc[df['tg_from'] == 'ООО "ГТ С-Петербург"',
       'tg_from'] = 'ООО "Газпром трансгаз Санкт-Петербург"'
df.loc[df['tg_from'] == 'ООО "ГТ Н. Новгород"',
       'tg_from'] = 'ООО "Газпром трансгаз Нижний Новгород"'

for city in cities:
    df.loc[df['tg_to_grs'].str.contains(
        '"ГТ '+city), 'tg_to_grs'] = 'ООО "Газпром трансгаз '+city+'"'
df.loc[df['tg_to_grs'] == 'ООО "ГТ С-Петербург"',
       'tg_to_grs'] = 'ООО "Газпром трансгаз Санкт-Петербург"'
df.loc[df['tg_to_grs'] == 'ООО "ГТ Н. Новгород"',
       'tg_to_grs'] = 'ООО "Газпром трансгаз Нижний Новгород"'

df.loc[df['tg_to_grs'] ==
       'КС Карпинская (на ГТ Чайковский)', 'tg_to_grs'] = 'ООО "Газпром трансгаз Чайковский"'
df.loc[df['tg_to_grs'] ==
       'КС Лялинская (на ГТ Чайковский)', 'tg_to_grs'] = 'ООО "Газпром трансгаз Чайковский"'
df.loc[df['tg_to_grs'] ==
       'КС Нижняя Тура (на ГТ Чайковский)', 'tg_to_grs'] = 'ООО "Газпром трансгаз Чайковский"'
df.loc[df['tg_to_grs'] == 'КС Нижняя Тура (на ГТ Екатеринбург)',
       'tg_to_grs'] = 'ООО "Газпром трансгаз Екатеринбург"'
df.loc[df['tg_to_grs'] ==
       'КС Приполярная (на ГТ Ухта)', 'tg_to_grs'] = 'ООО "Газпром трансгаз Ухта"'

df.drop(columns='city', inplace=True)

list_of_exceptions = list(df['tg_from'].unique())
list_of_exceptions.remove(
    'БП "622,5 км" (Локосово) (622,5 км газопровода Уренгой-Челябинск)')
list_of_exceptions.remove('КС Надым (216,2 км газопровода Уренгой-Петровск)')
add_list = ['ООО "Газпром трансгаз Краснодар"', 'ООО "Газпром трансгаз Томск"']
list_of_exceptions.extend(add_list)

stop_list = ['КС Надым (216,2 км газопровода Уренгой-Петровск)',
             'БП "622,5 км" (Локосово) (622,5 км газопровода Уренгой-Челябинск)']

new_df_data = []  # Список для хранения данных нового датафрейма

# Проходим циклом по всем строкам исходной таблицы df, чтобы отобрать строки для нового датафрейма new_df
for index, row in df.iterrows():
    tg_to_grs = row['tg_to_grs']
    if tg_to_grs in list_of_exceptions: # Проверка на принадлежность 'tg_to_grs' к промежуточным ГРС (если прнадлежность подтверждается - пропускаем такую строку)
        continue
    tg_owner = row['tg_owner']
    tg_dist = row['distance']
    
    new_row = {'ГРС': tg_to_grs, 'ГТ-0 (собственник)': tg_owner, 'расстояние_1': tg_dist}
    new_df_data.append(new_row)

new_df = pd.DataFrame(new_df_data)

new_df.sort_values(by=['ГРС'], inplace=True) # Сортируем датафрейм по ГРС (не обязательно)

# Проходим циклов по каждой строке полученного датафрейма для добавления новых столбцов
for index, row in new_df.iterrows():
    column_index = 1 
    tg_to_grs = row['ГРС']
    tg_owner = row['ГТ-0 (собственник)']
    tg_dist = df.loc[df['tg_to_grs'] == tg_to_grs, 'distance'].values[0]
    tg_from = df.loc[df['tg_to_grs'] == tg_to_grs, 'tg_from'].values[0]
    flag = True
    
    while flag:
        column_grs = f'ГТ-{column_index}'
        column_dist = f'расстояние_{column_index}'
        
        if tg_from in stop_list: # Проверка на наличие tg_from в списке конечных пунктов
            new_df.at[index, column_dist] = tg_dist
            new_df.at[index, column_grs] = tg_from
            flag = False
        else:
            new_df.at[index, column_dist] = tg_dist 
            new_df.at[index, column_grs] = tg_from 
            
            # Определяем новую активную строку исходя из условия, что tg_owner == tg_from, a tg_to_grs == tg_owner
            next_row = df.loc[(df['tg_owner'] == tg_from) & (df['tg_to_grs'] == tg_owner)]
            if len(next_row) > 1:
                next_row = next_row.loc[next_row['distance'] == next_row['distance'].min()] # Из нескольких одинаковых строк выбираем строку с наименьшим distance
            
            if len(next_row) == 0: # Если такой строки не находится, заканчиваем цикл
                flag = False
                break
            # Производим переназначение переменных исходя из новой активной строки
            tg_from = next_row['tg_from'].values[0]
            tg_to_grs = next_row['tg_to_grs'].values[0]
            tg_owner = next_row['tg_owner'].values[0]
            tg_dist = next_row['distance'].values[0]
            column_index += 1 # Увеличиваем индекс столбца на 1

new_df['Итого'] = new_df.filter(regex='^расстояние').sum(axis=1).round(2)


# Сохранение готовой таблицы маршрутов в заданную директорию.
with pd.ExcelWriter(
    "/Users/aleksejborovoi/Desktop/Python_projects/Gazprom/trans_gases.xlsx" # Здесь указывается ПОЛНЫЙ путь для сохранения готовой таблицы согласно примеру
    # Пример для Windows: r'C:\Users\Ron\Desktop\products.xlsx'
    # Пример для MacOS: '/Users/Ron/Desktop/products.xlsx' 
) as writer:
    new_df.to_excel(writer, sheet_name="Sheet1")  


# Визуализация (дашборд)


# Запускаем скрипт, если не установлена библиотека для дашбордов Dash, 
# нужно ее установить через терминал. Далее открываем браузер (любой), 
# вводим адрес http://127.0.0.1:8050/ и наблюдаем дашборд.

import dash
from dash import dcc, html
import dash_table
import pandas as pd

app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Dropdown(
        id='dropdown',
        options=[{'label': i, 'value': i} for i in new_df['ГРС'].unique()],
        value=[new_df['ГРС'].unique()[0]],
        multi=True
    ),
    dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i} for i in new_df.columns],
        data=new_df.to_dict('records')
    )
])

@app.callback(
    dash.dependencies.Output('table', 'data'),
    [dash.dependencies.Input('dropdown', 'value')])
def update_table(selected_values):
    dff = new_df[new_df['ГРС'].isin(selected_values)]
    return dff.to_dict('records')

if __name__ == '__main__':
    app.run_server(debug=True)
