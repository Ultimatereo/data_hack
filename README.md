# DATA HACK
## Кейс 2. Генератор Фейковый Данных для Сложных Запросов
### Информация про различные виды полей

Название класса | Его описание
---|---
SparkField | *абстрактный класс*, на основе которого строятся всевозможные виды полей 
Range |базовый класс для IntegerRange и FloatRange
IntegerRange | класс для генерации рандомного целого числа от start до stop
FloatRange | класс для генерации рандомного вещественного числа от start до stop
Constant | класс для генерации констант
StringRange | класс для генерации строк с заданным алфавитом длины от from_length до to_length
StringLen | класс для генерации строк с заданным алфавитом с фиксированной длиной
WeightSelect | класс для генерации чисел и строк из заданного множества готовых значений с частотным распределением
Select | класс для генерации чисел и строк из заданного множества готовых значений
Mask | базовый класс для IntegerMask и StringMask
IntegerMask | класс для генерации чисел с помощью масок
StringMask | класс для генерации строк с помощью масок
Time | базовый класс для Date и Timestamp
Date | класс для генерации данных типа date
Timestamp | класс для генерации данных типа timestamp

### Про поля настройки в конфигах таблиц

#### Ключи для настройки IntegerRange, FLoatRange

Ключ | Описание
:---:|:---:
start, stop | диапазон значений [start, stop], в рамках которых происходит генерация значений чисел

#### Ключи для настройки Constant

Ключ | Описание
:---:|:---:
value | значение константы

#### Ключи для настройки StringRange

Ключ | Описание
:---:|:---:
from_length, to_length | диапазог значений [from_length, to_length], в рамках которых происходит генерация длины строки
alphabet | строка символов (алфавит), из которых генерируется строка

#### Ключи для настройки StringLen

Ключ | Описание
:---:|:---:
length | длина генерируемой строки
alphabet | строка символов (алфавит), из которых генерируется строка

#### Ключи для настройки WeightSelect

Ключ | Описание
:---:|:---:
select | словарь, в котором ключ это строка или число, а значение это его приоритет встречаемости

#### Ключи для настройки Select

Ключ | Описание
:---:|:---:
select | множество (сет) значений, из которых выбирается случайным образом одно из них

#### Ключи для настройки IntegerMask, StringMask

Ключ | Описание
:---:|:---:
mask | строка, которая описывает маску. # отвечает за произвольный символ. Пример: "12fds###s"
alphabet | строка символов (алфавит), из которых генерируются буквы на место символа # в маске

#### Ключи для настройки Date

Ключ | Описание
:---:|:---:
start_year, start_month, start_day | дата date(start_year, start_month, start_day), начиная с которой происходит генерация дат
stop_year, stop_month, stop_day | дата date(end_year, end_month, end_day), заканчивая которой происходит генерация дат

#### Ключи для настройки TimeStamp

Ключ | Описание
:---:|:---:
start_year, start_month, start_day, start_hour, start_minute | время метка datetime(start_year, start_month, start_day, start_hour, start_minute), начиная с которой происходит генерация временных меток
stop_year, stop_month, stop_day, stop_hour, stop_minute | время метка datetime(end_year, end_month, end_day, end_hour, end_minute), заканчивая которой происходит генерация временных меток

#### Пример настроек сущности в конфиге
```
full_fields_config.json
{
  "FullFields": {
    "integerRange": {
      "start": -15,
      "stop": 40
    },
    "floatRange": {
      "start": -5,
      "stop": 5
    },
    "constant": {
      "value": 789
    },
    "stringRange": {
      "from_length": 2,
      "to_length": 4,
      "alphabet": "abcdef"
    },
    "stringLen": {
      "length": 5,
      "alphabet": "tyuiop"
    },
    "selectInt": {
      "select": [
        12,
        13,
        14
      ]
    },
    "selectFloat": {
      "select": [
        1.111,
        2.222,
        3.333,
        4.444,
        5.555
      ]
    },
    "weightSelectStr": {
      "select": {
        "superOften": 6,
        "superRare": 1
      }
    },
    "integerMask": {
      "mask": "789######",
      "alphabet": "1234"
    },
    "stringMask": {
      "mask": "######",
      "alphabet": "ABCDEF"
    },
    "dateField": {
      "start_year": 1988,
      "start_month": 2,
      "start_day": 6,
      "stop_year": 2000,
      "stop_month": 11,
      "stop_day": 11
    },
    "timestampField": {
      "start_year": 1988,
      "start_month": 2,
      "start_day": 6,
      "start_hour": 10,
      "start_minute": 32,
      "stop_year": 2000,
      "stop_month": 11,
      "stop_day": 11,
      "stop_hour": 10,
      "stop_minute": 32
    }
  }
}
```

### Про поля настройки в конфиге по запуску генератора

Ключ tasks отвечает за массив объектов, где каждый объект это последовательное действие, которое нужно выполнить. Каждое действие описывается ключом job, который определяет тип действия

Значение ключа job в объекте | Описание действия
:---:|:---:
solo_generate | Сгенерировать таблицу
paired_generate | Сгенерировать две связанные таблицы
show_date | Показать данные в консоли

#### Ключи настройки объекта типа solo_generate

Ключ | Описание
:---:|:---:
args | объект объектов, которые описываются ниже
table (script_name, class_name) | объект table с ключами script_name и class_name, которые дают информацию о том, что таблица находится в скрипте script.name, и датаклассе, описывающем класс, это class_name
export (dir_name, data_type, mode) | объект export с ключами dir_name, data_type, mode, которые дают информацию о том, что таблица будет сохраняться в папке dir_name типа data_type (по дефолту parquet, возможные значения csv, json, parquet), mode (по дефолту overwrite) - режим записи (append, overwrite, ignore)
config | название файла конфига для таблицы

#### Ключи настройки объекта типа paired_generate

Ключ | Описание
:---:|:---:
args | объект объектов, которые описываются ниже
table1, table2 (script_name, class_name) | объекты table1, table2 с ключами script_name и class_name, которые дают информацию о том, что таблицы находится в скрипте script.name, и датаклассе, описывающем класс, это class_name
export (dir_name1, dir_name2, data_type, mode) | объект export с ключами dir_name1, dir_name2, data_type, mode, которые дают информацию о том, что таблицы будет сохраняться в папках dir_name1 и dir_name2 соответственно типа data_type (по дефолту parquet, возможные значения csv, json, parquet), mode (по дефолту overwrite) - режим записи (append, overwrite, ignore)
intersect_keys("a1" : "b1", "a2" : "b2", ...) | объект, который сопоставляет поле a_i из первой таблицы полю b_i из второй таблицы
config | название файла конфига для обеих таблиц

#### Ключи настройки объекта типа show_data
Ключ | Описание
:---:|:---:
args | объект объектов, которые описываются ниже
dir_name | имя папки, из которой демонстрируется таблица
data_type | тип данных, в котором хранится таблица
count | количество первых строк столбца, которые надо вывести в консоль (по дефолту 20)

#### Пример настроек запуска генератора

```
default.json
{
  "tasks": [
    {
      "job": "solo_generate",
      "args": {
        "table": {
          "script_name": "CellClass",
          "class_name": "Cell"
        },
        "export": {
          "dir_name": "CellGen",
          "data_type": "parquet",
          "mode": "append"
        },
        "config": "config.json"
      }
    },
    {
      "job": "show_data",
      "args": {
        "dir_name": "CellGen",
        "data_type": "parquet",
        "count": 5
      }
    },
    {
      "job": "paired_generate",
      "args": {
        "table1": {
          "script_name": "CellClass",
          "class_name": "Cell"
        },
        "table2": {
          "script_name": "Cell2Class",
          "class_name": "Cell2"
        },
        "intersect_keys": {
          "integer1": "iid",
          "float2": "acceleration",
          "abcword": "word"
        },
        "export": {
          "dir_name1": "CellPrd",
          "dir_name2": "Cell2Prd",
          "data_type": "parquet"
        },
        "config": "config.json"
      }
    },
    {
      "job": "show_data",
      "args": {
        "dir_name": "CellPrd",
        "data_type": "parquet",
        "count": 10
      }
    },
    {
      "job": "show_data",
      "args": {
        "dir_name": "Cell2Prd",
        "data_type": "parquet",
        "count": 10
      }
    }
  ]
}
```
