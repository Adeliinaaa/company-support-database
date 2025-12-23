# Документация по данным компаний

*Дата создания: 23.12.2025 17:50*

## Описание данных

Данный файл содержит информацию о компаниях с оценкой качества данных.

## Структура данных

| Колонка | Описание | Тип данных | Пример |
|---------|----------|------------|--------|
| company_id | Уникальный идентификатор компании | object | C0029 |
| name | Название компании | object | Ингосстрах |
| normalized_name | Не описано | object | ИНГОССТРАХ |
| industry | Отрасль деятельности | object | insurance |
| inn | Не описано | object | 7714017986 |
| primary_site | Основной сайт компании | object | https://qr.nspk.ru/BS1A003B1PB... |
| primary_email | Основной email для связи | object | www@ingos.ru |
| data_quality_score | Оценка качества данных (0-100) | int64 | 100 |
| has_support_team | Не описано | bool | True |
| support_team_size | Размер команды поддержки | int64 | 10 |
| support_channels_count | Количество каналов поддержки | int64 | 5 |
| has_24_7_support | Наличие круглосуточной поддержки | bool | True |
| support_vacancies | Не описано | int64 | 3 |
| total_vacancies | Не описано | int64 | 3 |
| data_sources | Не описано | object | enriched, jobs_detailed, jobs_... |
| description | Не описано | object | Отрасль: insurance | Размер ко... |
| emails | Не описано | object | ['www@ingos.ru'] |
| last_updated | Не описано | object | 2025-12-23T17:34:28.091739 |
| metadata | Не описано | object | {'parsing_success': True, 'par... |
| sites | Не описано | object | ['https://qr.nspk.ru/BS1A003B1... |
| source | Не описано | object | seed |
| sources | Не описано | object | ['enriched', 'jobs_detailed', ... |
| support_info | Не описано | object | {'team_size': 10, 'channels': ... |

## Использование

Данные могут быть использованы для:
- Анализа рынка
- Построения системы поддержки клиентов
- Исследования отраслевых тенденций
- Оценки качества данных компаний
