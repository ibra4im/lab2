
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import csv
import re

context = iter(ET.iterparse("E:\\OBV_full.xml", events=("start", "end")))
# записываем данные из файла в переменную context
#создаем массивы
cols = [] # все столбцы
path = [] #путь
num = 0
counter = 0

for event, elem in context: # проходим все элементы массива
  if event == "start": # если запись уже начали
    num = num + 1 # увеличиваем счетчик 
    path.append(elem.tag) # добавляем в массив "путь"
    path_str = '>'.join(path)# превращаем путь из массива в строку 
    if path_str not in cols: # если такой пути нет
      cols.append(path_str)# то добавляем его.

  if event == "end": # если запись закончена 
    num = num - 1 # уменьшаем счетчик 
    path_str = '>'.join(path) # превращаем путь из массива в строку

    path.pop() #убираем все элементы из массива 
    if num == 2: # если это вакансия 
      counter = counter + 1 # счетчик вансии увеличиваем 
      if counter % 10000 == 0: # выводим каждые 10 000 вакансий.
        print(counter)



cols.remove('source>vacancies>vacancy>salary') # удаляем зп
cols.append('source>vacancies>vacancy>salary-max') # добавляем мах зп
cols.append('source>vacancies>vacancy>salary-min')# добавляем мин зп

result_cols = [] # массив столбцов

for i in cols: # тут проходим массив столбцов
  n = True 
  for j in cols: 
    if i!=j:
      if j.startswith(i):
        n = False
        break
  if n:
    result_cols.append(i) # добавляем нужные столбцы в масив столбцов

print(result_cols) # вывод столбцов

cols_i = {} # индексы столбцов
col_cnt = len(result_cols)# общее кол - во столбцов 

for i in range(col_cnt): 
  cols_i[result_cols[i]] = i # записываем индекс столбцов 

context = iter(ET.iterparse("E:\\OBV_full.xml", events=("start", "end"))) # 

zfrom = re.compile("от (\d+)", re.IGNORECASE) # подготавливаем регулярное выражение для получения мин зп
zto = re.compile("до (\d+)", re.IGNORECASE) # подготов регул выражение для получения мах зп

zmaxi = cols_i['source>vacancies>vacancy>salary-max'] # получение индексов для этих столбцов
zmini = cols_i['source>vacancies>vacancy>salary-min']

counter = 0 # счетчик вакансий

depth = 0 # глубина пути

rows = [] #строки в таблице

for event, elem in context:
  if event == "start": 
    path.append(elem.tag) 
    depth = depth + 1
    if depth == 3:  
      row = [np.nan] * col_cnt # создаем новую строку с пустыми значениями

  path_str = '>'.join(path) # 

  if event == "end":
    path.pop()
    depth = depth - 1
    if depth == 2: # если закончился вакансия 
      rows.append(row) # то добавляет новую строку с вакансией в массив со строками

      counter = counter + 1 
    else:
      if elem.text and not elem.text.isspace(): # если у атрибута xml есть текст и он пустой
        if path_str == 'source>vacancies>vacancy>salary': # если текущий элемент зп
          s = zto.search(elem.text) # ищем мах зп
          val = s.group(1) if s else np.nan #  если есть записываем s если нет то пустое значение
          if not pd.isna(row[zmaxi]): # если уже есть в столбце зп
            row[zmaxi] = row[zmaxi]+';'+val # через ; записываем
          else: 
            row[zmaxi] = val # если не было ничего то просто записыаем 
          s = zfrom.search(elem.text)# для мин зп
          val = s.group(1) if s else np.nan# 
          if not pd.isna(row[zmini]):#
            row[zmini] = row[zmini]+';'+val#
          else:
            row[zmini] = val # если ничего не было то просто записываем
        val = elem.text # значение переменной
        if path_str == 'source>vacancies>vacancy>job-name': # если название вакансии
          val = val.replace(',',';') # заменяем 
        if path_str in cols_i: # если такой столбец есть
          index = cols_i[path_str] # получаем индекс
          if not pd.isna(row[index]): # если там есть значение 
            row[index] = row[index]+';'+val # записыаем через ; 
          else:
            row[index] = val 
      

result_df = pd.DataFrame(rows,columns=result_cols) # создаем датафрейм с такими столбцами и строками
print(result_df)
result_df.to_csv("E:\\lab1_result2.csv",  na_rep = 'NA', index=True, index_label="",quotechar='"',quoting=csv.QUOTE_NONNUMERIC,encoding='utf-8-sig')
