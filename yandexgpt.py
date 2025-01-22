import pandas as pd
import requests

from sqlalchemy import text


class YandexGPT:
    def __init__(self, api_key, folder_id, engine):
        self.api_key = api_key
        self.folder_id = folder_id
        self.url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Api-Key {self.api_key}"
        }
        self.last_question = ""
        self.last_sql = ""
        self.last_result = ""

        self.engine = engine

    
    def invoke(self, question):
        sql = self._text2sql(question)
        # try:
        #     rows = self._get_data(sql)
        # except Exception as e:
        #      print("Вызвалось исключение в базе")
        #      return sql
        # rows = self._get_data(sql)

        rows = self._get_data(sql)
        columns = self._get_columns(sql)
        result = self._final_answer(question, sql, columns, rows)
        return result


    def _text2sql(self, question):

        prompt = {
            "modelUri": f"gpt://{self.folder_id}/yandexgpt",
            "completionOptions": {
                "stream": False,
                "temperature": 0.0,
                "maxTokens": 4000
            },
            "messages": [
                {
                    "role": "system",
                    "text": '''Ты специалист в SQL. Создаешь запросы SQL к таблице с архивами погоды'''
                },
                {
                    "role": "user",
                    "text": f''' Учитывая входной вопрос, создайте синтаксически правильный запрос SQLite для запуска.
                        Никогда не запрашивайте все столбцы таблицы. Вы должны запрашивать только те столбцы, которые необходимы для ответа на вопрос. 
                        Обратите внимание: используйте только те имена столбцов, которые вы видите в таблицах ниже. Будьте осторожны и не запрашивайте несуществующие столбцы. 
                        Также обратите внимание, какой столбец в какой таблице находится.
                        Обратите внимание на использование функции date('now') для получения текущей даты, если вопрос касается «сегодня».
                        Всегда используй формат '%Y-%m-%d %H:00:00.000000' рядом с date.
                        Например 25 декабря 2024 14 часов - это '2024-12-25 14:00:00.000000'
                        Старайся использовать between когда речь идет об одной дате
                        Если вопрос об истории погоды, возвращай ТОЛЬКО SQL запрос без дополнительных символов начиная с SELECT
                        Используйте только следующие таблицы и комментарий к столбцам для ответа:
                        CREATE TABLE my_table (
                            "index" BIGINT, 
                            date TIMESTAMP "Дата события с указанием даты и часа", 
                            temperature FLOAT "Температура воздуха", 
                            rain FLOAT "Дождь в миллиметрах осадков", 
                            snowfall FLOAT "Снегопад в миллиметрах осадков", 
                            wind_speed FLOAT "Скорость ветра", 
                            wind_direction FLOAT "Направление ветра"
                        )
                        3 rows from my_table table:
                        index	date	           temperature	    rain	snowfall	       wind_speed	     wind_direction
                    Учитывай также предыдущий вопрос:
                    Q: {self.last_question}
                    A: {self.last_sql}
                    A: {question}
                    '''
                }
            ]
        }

        response = requests.post(self.url, headers=self.headers, json=prompt)
        result = response.json()
        sql = result['result']['alternatives'][0]['message']['text']
        sql = sql.replace("```\n", "")
        sql = sql.replace("\n```", "")

        self.last_sql = sql
        #self.last_question = question

        return sql
    

    def _get_data(self, sql):
        try:
            rows = ""
            with self.engine.connect() as con:
                rs = con.execute(text(sql))
                for row in rs:
                        # print(row)
                    rows += str(row) + "\n"
        except Exception as e:
            return 'NOT SQL'
        return rows


    def _get_columns(self, sql):

        prompt = {
            "modelUri": f"gpt://{self.folder_id}/yandexgpt",
            "completionOptions": {
                "stream": False,
                "temperature": 0,
                "maxTokens": "2000"
            },
            "messages": [
                {
                    "role": "assistant",
                    "text": f'''
                            Вытащи названия колонок из запроса. Верни их названия
                            {sql}
                        Если запрос начинается с SELECT * FROM, то верни (index, date, temperature, rain, snow, wind_speed, wind_direction)
                            '''
                }
            ]
        }
        response = requests.post(self.url, headers=self.headers, json=prompt)
        result = response.json()
        columns = result['result']['alternatives'][0]['message']['text']
        return columns
                     

    
    def _final_answer(self, question, sql, columns, rows):

        prompt = {
            "modelUri": f"gpt://{self.folder_id}/yandexgpt",
            "completionOptions": {
                "stream": False,
                "temperature": 0,
                "maxTokens": "4000"
            },
            "messages": [
                {
                    "role": "system",
                    "text": f'''Ты получаешь на вход вопрос пользователя, SQL запрос к таблице с архивами погоды и результат его выполнения в виде 
                            списка строк из таблицы
                            Верни ответ о погоде, опираясь на эти параметры, а также на описание колонок таблицы
                            CREATE TABLE my_table (
                                "index" BIGINT, 
                                date TIMESTAMP "Дата события с указанием даты и часа", 
                                temperature FLOAT "Температура воздуха", 
                                rain FLOAT "Дождь в миллиметрах осадков", 
                                snowfall FLOAT "Снегопад в миллиметрах осадков", 
                                wind_speed FLOAT "Скорость ветра", 
                                wind_direction FLOAT "Направление ветра"
                            )
                            Если вопрос пользователя не о истории погоды, пиши что ты умеешь отвечать о погоде
                            '''
                },
                {
                    "role": "user",
                    "text": f'''
                            Question: {question}
                            SQL: {sql}
                            Result: {(columns)}
                            {rows}

                            '''
                }
            ]
        }
        #print(sql)
        #print((columns))
        self.last_question = question

        response = requests.post(self.url, headers=self.headers, json=prompt)
        result = response.json()
        self.last_result = result['result']['alternatives'][0]['message']['text']
        return result['result']['alternatives'][0]['message']['text']
        


