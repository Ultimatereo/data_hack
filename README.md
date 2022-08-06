# DATA HACK
## Кейс 2. Генератор Фейковый Данных для Сложных Запросов
### Информация про различные виды полей

Название класса | Его описание
---|---
SparkField | *абстрактный класс*, на основе которого строятся всевозможные виды полей 
Range |базовый класс для IntegerRange и FloatRange
IntegerRange | класс для генерации рандомного целого числа от start до stop
StringRange | класс для генерации рандомного вещественного числа от start до stop
Constant | класс для генерации констант
StringRange | класс для генерации строк с заданным алфавитом длины от from_length до to_length
StringLen | класс для генерации строк с заданным алфавитом с фиксированной длиной
Select | класс для генерации чисел и строк из заданного набора значений
Mask | базовый класс для IntegerMask и StringMask
IntegerMask | класс для генерации чисел с помощью масок
StringMask | класс для генерации строк с помощью масок
Time | базовый класс для Date и Timestamp
Date | класс для генерации данных типа date
Timestamp | класс для генерации данных типа timestamp
