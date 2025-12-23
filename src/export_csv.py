"""
Модуль для экспорта данных компаний в различные форматы.
Предоставляет гибкие возможности экспорта с фильтрацией, сортировкой и форматированием.
"""

import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CompanyDataExporter:
    """Класс для экспорта данных компаний в различные форматы"""

    def __init__(self, data_path: str = 'data/processed/'):
        """
        Инициализация экспортера

        Args:
            data_path: Путь к папке с обработанными данными
        """
        self.data_path = data_path
        self.latest_file = self._find_latest_file()

    def _find_latest_file(self) -> Optional[str]:
        """Найти последний обработанный файл"""
        try:
            if not os.path.exists(self.data_path):
                return None

            # Ищем файлы с компаниями
            company_files = [f for f in os.listdir(self.data_path)
                             if f.startswith('companies_') and f.endswith('.csv')]

            if not company_files:
                return None

            # Сортируем по дате (самый новый первый)
            company_files.sort(reverse=True)

            # Возвращаем самый новый полный файл
            for file in company_files:
                if 'master_dataset' in file or 'complete' in file:
                    return os.path.join(self.data_path, file)

            # Если не нашли master_dataset, берем первый
            return os.path.join(self.data_path, company_files[0])

        except Exception as e:
            logger.error(f"Ошибка при поиске файла: {e}")
            return None

    def load_data(self, file_path: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Загрузка данных из файла

        Args:
            file_path: Путь к файлу (если None, используется последний)

        Returns:
            DataFrame с данными или None при ошибке
        """
        if file_path is None:
            file_path = self.latest_file

        if file_path is None or not os.path.exists(file_path):
            logger.error(f"Файл не найден: {file_path}")
            return None

        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            logger.info(f"Загружено {len(df)} записей из {file_path}")
            return df
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {e}")
            return None

    def export_to_formats(self, df: pd.DataFrame, export_name: str = "export") -> Dict[str, str]:
        """
        Экспорт данных во все форматы

        Args:
            df: DataFrame с данными
            export_name: Базовое имя для экспорта

        Returns:
            Словарь с путями к созданным файлам
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        export_dir = f'exports/{timestamp}_{export_name}'
        os.makedirs(export_dir, exist_ok=True)

        export_paths = {}

        # 1. CSV со всеми данными
        csv_path = os.path.join(export_dir, f'{export_name}_full.csv')
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        export_paths['csv_full'] = csv_path

        # 2. Excel с несколькими листами
        excel_path = os.path.join(export_dir, f'{export_name}_dashboard.xlsx')
        self._export_to_excel(df, excel_path)
        export_paths['excel'] = excel_path

        # 3. JSON для веб-приложений
        json_path = os.path.join(export_dir, f'{export_name}_data.json')
        self._export_to_json(df, json_path)
        export_paths['json'] = json_path

        # 4. HTML отчет
        html_path = os.path.join(export_dir, f'{export_name}_report.html')
        self._export_to_html(df, html_path)
        export_paths['html'] = html_path

        # 5. Markdown документация
        md_path = os.path.join(export_dir, f'{export_name}_README.md')
        self._export_to_markdown(df, md_path)
        export_paths['markdown'] = md_path

        logger.info(f"Данные экспортированы в {export_dir}")
        return export_paths

    def export_filtered(self, df: pd.DataFrame, filters: Dict[str, Any],
                        export_name: str = "filtered") -> Optional[pd.DataFrame]:
        """
        Экспорт отфильтрованных данных

        Args:
            df: Исходные данные
            filters: Словарь с фильтрами
            export_name: Имя для экспорта

        Returns:
            Отфильтрованный DataFrame
        """
        try:
            filtered_df = df.copy()

            # Применяем фильтры
            for column, value in filters.items():
                if column in filtered_df.columns:
                    if isinstance(value, (list, tuple)):
                        # Фильтр по списку значений
                        filtered_df = filtered_df[filtered_df[column].isin(value)]
                    elif isinstance(value, dict):
                        # Сложный фильтр (например, диапазон)
                        if 'min' in value and 'max' in value:
                            filtered_df = filtered_df[
                                (filtered_df[column] >= value['min']) &
                                (filtered_df[column] <= value['max'])
                                ]
                    else:
                        # Простое равенство
                        filtered_df = filtered_df[filtered_df[column] == value]

            if len(filtered_df) > 0:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                export_dir = f'exports/{timestamp}_{export_name}'
                os.makedirs(export_dir, exist_ok=True)

                export_path = os.path.join(export_dir, f'{export_name}_data.csv')
                filtered_df.to_csv(export_path, index=False, encoding='utf-8-sig')

                logger.info(f"Экспортировано {len(filtered_df)} записей в {export_path}")
                return filtered_df
            else:
                logger.warning("Нет данных, соответствующих фильтрам")
                return None

        except Exception as e:
            logger.error(f"Ошибка при фильтрации: {e}")
            return None

    def export_by_quality(self, df: pd.DataFrame, min_score: int = 0,
                          max_score: int = 100) -> Dict[str, pd.DataFrame]:
        """
        Экспорт данных по качеству

        Args:
            df: Исходные данные
            min_score: Минимальная оценка
            max_score: Максимальная оценка

        Returns:
            Словарь с DataFrames по категориям качества
        """
        if 'data_quality_score' not in df.columns:
            logger.error("В данных отсутствует колонка data_quality_score")
            return {}

        results = {}

        # Определяем категории качества
        categories = [
            ('excellent', 80, 100, 'Отличное качество (80-100)'),
            ('good', 60, 79, 'Хорошее качество (60-79)'),
            ('average', 40, 59, 'Среднее качество (40-59)'),
            ('poor', 0, 39, 'Низкое качество (0-39)')
        ]

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        export_dir = f'exports/{timestamp}_by_quality'
        os.makedirs(export_dir, exist_ok=True)

        for cat_id, min_cat, max_cat, cat_name in categories:
            cat_df = df[
                (df['data_quality_score'] >= min_cat) &
                (df['data_quality_score'] <= max_cat)
                ].copy()

            if len(cat_df) > 0:
                cat_df = cat_df.sort_values('data_quality_score', ascending=False)

                # Экспорт категории
                export_path = os.path.join(export_dir, f'companies_{cat_id}_{len(cat_df)}.csv')
                cat_df.to_csv(export_path, index=False, encoding='utf-8-sig')

                results[cat_id] = cat_df
                logger.info(f"{cat_name}: {len(cat_df)} компаний")

        # Сводный отчет
        summary_path = os.path.join(export_dir, 'quality_summary.md')
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write("# Сводка по качеству данных\n\n")
            for cat_id, min_cat, max_cat, cat_name in categories:
                if cat_id in results:
                    count = len(results[cat_id])
                    percentage = (count / len(df)) * 100
                    f.write(f"## {cat_name}\n")
                    f.write(f"- Количество компаний: {count} ({percentage:.1f}%)\n")
                    f.write(f"- Диапазон оценок: {min_cat}-{max_cat}\n")

                    if len(results[cat_id]) > 0:
                        avg_score = results[cat_id]['data_quality_score'].mean()
                        f.write(f"- Средняя оценка: {avg_score:.1f}\n")

                        top_3 = results[cat_id].head(3)
                        f.write("\n### Топ-3 компании:\n")
                        for idx, row in top_3.iterrows():
                            f.write(f"- **{row.get('name', 'Без названия')}**: {row['data_quality_score']}/100\n")

                    f.write("\n")

        return results

    def export_analysis_report(self, df: pd.DataFrame,
                               report_name: str = "analysis_report") -> str:
        """
        Создание комплексного аналитического отчета

        Args:
            df: Данные для анализа
            report_name: Имя отчета

        Returns:
            Путь к созданному отчету
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        export_dir = f'exports/{timestamp}_{report_name}'
        os.makedirs(export_dir, exist_ok=True)

        report_path = os.path.join(export_dir, f'{report_name}.md')

        with open(report_path, 'w', encoding='utf-8') as f:
            # Заголовок отчета
            f.write(f"# Аналитический отчет по данным компаний\n\n")
            f.write(f"**Дата генерации:** {datetime.now().strftime('%d.%m.%Y %H:%M')}\n")
            f.write(f"**Всего компаний:** {len(df)}\n\n")

            # 1. Общая статистика
            f.write("## 1. Общая статистика\n\n")

            if 'data_quality_score' in df.columns:
                f.write(f"- **Средняя оценка качества:** {df['data_quality_score'].mean():.1f}/100\n")
                f.write(f"- **Медианная оценка:** {df['data_quality_score'].median():.1f}/100\n")
                f.write(f"- **Стандартное отклонение:** {df['data_quality_score'].std():.1f}\n")
                f.write(f"- **Минимальная оценка:** {df['data_quality_score'].min():.0f}/100\n")
                f.write(f"- **Максимальная оценка:** {df['data_quality_score'].max():.0f}/100\n\n")

            # 2. Заполненность полей
            f.write("## 2. Заполненность полей\n\n")

            fields_to_check = [
                ('name', 'Название компании'),
                ('industry', 'Отрасль'),
                ('primary_site', 'Сайт'),
                ('primary_email', 'Email'),
                ('support_team_size', 'Размер команды поддержки'),
                ('data_quality_score', 'Оценка качества')
            ]

            for field, description in fields_to_check:
                if field in df.columns:
                    filled = df[field].notna().sum()
                    percentage = (filled / len(df)) * 100
                    f.write(f"- **{description}:** {filled}/{len(df)} ({percentage:.1f}%)\n")

            f.write("\n")

            # 3. Анализ поддержки (если есть данные)
            support_fields = ['has_support_team', 'support_team_size', 'support_channels_count']
            if any(field in df.columns for field in support_fields):
                f.write("## 3. Анализ поддержки\n\n")

                if 'has_support_team' in df.columns:
                    with_support = df['has_support_team'].sum()
                    percentage = (with_support / len(df)) * 100
                    f.write(f"- **Компании с командой поддержки:** {with_support} ({percentage:.1f}%)\n")

                if 'support_team_size' in df.columns:
                    avg_team = df[df['support_team_size'] > 0]['support_team_size'].mean()
                    if not pd.isna(avg_team):
                        f.write(f"- **Средний размер команды поддержки:** {avg_team:.1f} человек\n")

                if 'support_channels_count' in df.columns:
                    avg_channels = df['support_channels_count'].mean()
                    f.write(f"- **Среднее количество каналов поддержки:** {avg_channels:.1f}\n")

                f.write("\n")

            # 4. Топ-10 компаний
            f.write("## 4. Топ-10 компаний по качеству данных\n\n")

            if 'data_quality_score' in df.columns and 'name' in df.columns:
                top_10 = df.nlargest(10, 'data_quality_score')

                f.write("| Ранг | Название | Оценка | Отрасль | Размер команды |\n")
                f.write("|------|----------|--------|---------|----------------|\n")

                for idx, (_, row) in enumerate(top_10.iterrows(), 1):
                    name = row.get('name', 'Без названия')[:40]
                    score = row['data_quality_score']
                    industry = row.get('industry', 'Не указана')[:20]
                    team_size = row.get('support_team_size', 0)

                    f.write(f"| {idx} | {name} | {score:.0f}/100 | {industry} | {team_size} |\n")

                f.write("\n")

            # 5. Рекомендации
            f.write("## 5. Рекомендации\n\n")

            recommendations = []

            # Проверяем наличие низких оценок
            if 'data_quality_score' in df.columns:
                low_quality = df[df['data_quality_score'] < 50]
                if len(low_quality) > 0:
                    recommendations.append(
                        f"- **{len(low_quality)} компаний** имеют оценку ниже 50. "
                        f"Рекомендуется провести дополнительный сбор данных."
                    )

            # Проверяем отсутствие контактных данных
            if 'primary_email' in df.columns:
                no_email = df[df['primary_email'].isna()].shape[0]
                if no_email > 0:
                    recommendations.append(
                        f"- **{no_email} компаний** не имеют email. "
                        f"Рекомендуется провести парсинг сайтов."
                    )

            if 'primary_site' in df.columns:
                no_site = df[df['primary_site'].isna()].shape[0]
                if no_site > 0:
                    recommendations.append(
                        f"- **{no_site} компаний** не имеют сайта. "
                        f"Рекомендуется проверить исходные данные."
                    )

            # Добавляем общие рекомендации
            recommendations.extend([
                "- Регулярно обновлять данные (минимум раз в квартал)",
                "- Внедрить автоматический мониторинг изменений на сайтах компаний",
                "- Добавить сбор дополнительных метрик (отзывы, рейтинги)",
                "- Интегрировать с CRM системой для отслеживания взаимодействий"
            ])

            for rec in recommendations:
                f.write(f"{rec}\n")

            # 6. Сводные таблицы (экспорт в CSV)
            f.write("\n## 6. Сводные данные\n\n")
            f.write("Сводные таблицы экспортированы в отдельные файлы:\n\n")

            # Экспорт сводных данных
            summary_files = []

            # По отраслям
            if 'industry' in df.columns and 'data_quality_score' in df.columns:
                industry_summary = df.groupby('industry').agg({
                    'data_quality_score': ['count', 'mean', 'min', 'max'],
                    'support_team_size': 'mean'
                }).round(2)

                industry_path = os.path.join(export_dir, 'industry_summary.csv')
                industry_summary.to_csv(industry_path, encoding='utf-8-sig')
                summary_files.append(("По отраслям", "industry_summary.csv"))

            # По качеству
            if 'data_quality_score' in df.columns:
                quality_bins = [0, 30, 50, 70, 90, 100]
                quality_labels = ['Очень низкое', 'Низкое', 'Среднее', 'Высокое', 'Очень высокое']

                df['quality_category'] = pd.cut(
                    df['data_quality_score'],
                    bins=quality_bins,
                    labels=quality_labels,
                    right=False
                )

                quality_summary = df['quality_category'].value_counts().sort_index()
                quality_path = os.path.join(export_dir, 'quality_summary.csv')
                quality_summary.to_csv(quality_path, encoding='utf-8-sig')
                summary_files.append(("По качеству", "quality_summary.csv"))

            for title, filename in summary_files:
                f.write(f"- [{title}]({filename})\n")

        logger.info(f"Аналитический отчет создан: {report_path}")
        return report_path

    def _export_to_excel(self, df: pd.DataFrame, excel_path: str):
        """Экспорт в Excel с несколькими листами"""
        try:
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # Лист 1: Все данные
                df.to_excel(writer, sheet_name='Все компании', index=False)

                # Лист 2: Топ компаний
                if 'data_quality_score' in df.columns:
                    top_50 = df.nlargest(50, 'data_quality_score')
                    top_50.to_excel(writer, sheet_name='Топ-50 компаний', index=False)

                # Лист 3: Статистика
                stats_data = self._generate_statistics(df)
                stats_df = pd.DataFrame(stats_data)
                stats_df.to_excel(writer, sheet_name='Статистика', index=False)

                # Лист 4: По отраслям
                if 'industry' in df.columns:
                    industry_stats = df.groupby('industry').agg({
                        'data_quality_score': 'mean',
                        'support_team_size': 'mean',
                        'name': 'count'
                    }).round(2)
                    industry_stats.to_excel(writer, sheet_name='По отраслям')

                # Автонастройка ширины колонок
                for sheet_name in writer.sheets:
                    worksheet = writer.sheets[sheet_name]
                    for column in worksheet.columns:
                        max_length = 0
                        column_letter = column[0].column_letter
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except:
                                pass
                        adjusted_width = min(max_length + 2, 50)
                        worksheet.column_dimensions[column_letter].width = adjusted_width

        except Exception as e:
            logger.error(f"Ошибка при экспорте в Excel: {e}")

    def _export_to_json(self, df: pd.DataFrame, json_path: str):
        """Экспорт в JSON формат"""
        try:
            # Конвертируем DataFrame в список словарей
            data = df.to_dict(orient='records')

            # Добавляем метаданные
            export_data = {
                'metadata': {
                    'export_date': datetime.now().isoformat(),
                    'total_companies': len(df),
                    'columns': list(df.columns)
                },
                'companies': data
            }

            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2)

        except Exception as e:
            logger.error(f"Ошибка при экспорте в JSON: {e}")

    def _export_to_html(self, df: pd.DataFrame, html_path: str):
        """Экспорт в HTML отчет"""
        try:
            html_content = """
            <!DOCTYPE html>
            <html lang="ru">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Отчет по компаниям</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 40px; }
                    h1 { color: #333; }
                    table { border-collapse: collapse; width: 100%; margin: 20px 0; }
                    th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
                    th { background-color: #f4f4f4; }
                    tr:nth-child(even) { background-color: #f9f9f9; }
                    .stats { background-color: #e8f4f8; padding: 20px; border-radius: 5px; }
                </style>
            </head>
            <body>
                <h1>📊 Отчет по данным компаний</h1>
            """

            # Статистика
            html_content += '<div class="stats">'
            html_content += f'<h3>Общая статистика</h3>'
            html_content += f'<p><strong>Всего компаний:</strong> {len(df)}</p>'

            if 'data_quality_score' in df.columns:
                avg_score = df['data_quality_score'].mean()
                html_content += f'<p><strong>Средняя оценка качества:</strong> {avg_score:.1f}/100</p>'

            html_content += '</div>'

            # Таблица с данными (первые 50 строк)
            html_content += '<h3>Данные компаний (первые 50)</h3>'
            html_content += df.head(50).to_html(index=False, classes='data-table')

            html_content += """
                <script>
                    // Простая сортировка таблицы
                    document.addEventListener('DOMContentLoaded', function() {
                        const tables = document.querySelectorAll('table');
                        tables.forEach(table => {
                            const headers = table.querySelectorAll('th');
                            headers.forEach((header, index) => {
                                header.style.cursor = 'pointer';
                                header.addEventListener('click', () => {
                                    sortTable(table, index);
                                });
                            });
                        });
                    });

                    function sortTable(table, column) {
                        const tbody = table.querySelector('tbody');
                        const rows = Array.from(tbody.querySelectorAll('tr'));

                        rows.sort((a, b) => {
                            const aText = a.children[column].textContent;
                            const bText = b.children[column].textContent;

                            // Пытаемся сравнить как числа
                            const aNum = parseFloat(aText.replace(',', '.'));
                            const bNum = parseFloat(bText.replace(',', '.'));

                            if (!isNaN(aNum) && !isNaN(bNum)) {
                                return aNum - bNum;
                            }

                            // Иначе сравниваем как строки
                            return aText.localeCompare(bText);
                        });

                        // Очищаем и добавляем отсортированные строки
                        rows.forEach(row => tbody.appendChild(row));
                    }
                </script>
            </body>
            </html>
            """

            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)

        except Exception as e:
            logger.error(f"Ошибка при экспорте в HTML: {e}")

    def _export_to_markdown(self, df: pd.DataFrame, md_path: str):
        """Экспорт в Markdown документацию"""
        try:
            with open(md_path, 'w', encoding='utf-8') as f:
                f.write("# Документация по данным компаний\n\n")
                f.write(f"*Дата создания: {datetime.now().strftime('%d.%m.%Y %H:%M')}*\n\n")

                f.write("## Описание данных\n\n")
                f.write("Данный файл содержит информацию о компаниях с оценкой качества данных.\n\n")

                f.write("## Структура данных\n\n")
                f.write("| Колонка | Описание | Тип данных | Пример |\n")
                f.write("|---------|----------|------------|--------|\n")

                # Описание колонок
                column_descriptions = {
                    'company_id': 'Уникальный идентификатор компании',
                    'name': 'Название компании',
                    'industry': 'Отрасль деятельности',
                    'primary_site': 'Основной сайт компании',
                    'primary_email': 'Основной email для связи',
                    'data_quality_score': 'Оценка качества данных (0-100)',
                    'support_team_size': 'Размер команды поддержки',
                    'support_channels_count': 'Количество каналов поддержки',
                    'has_24_7_support': 'Наличие круглосуточной поддержки'
                }

                for column in df.columns:
                    description = column_descriptions.get(column, 'Не описано')
                    dtype = str(df[column].dtype)

                    # Пример значения (первое непустое)
                    example = df[column].dropna().iloc[0] if not df[column].isna().all() else 'Нет данных'
                    if isinstance(example, str) and len(example) > 30:
                        example = example[:30] + '...'

                    f.write(f"| {column} | {description} | {dtype} | {example} |\n")

                f.write("\n## Использование\n\n")
                f.write("Данные могут быть использованы для:\n")
                f.write("- Анализа рынка\n")
                f.write("- Построения системы поддержки клиентов\n")
                f.write("- Исследования отраслевых тенденций\n")
                f.write("- Оценки качества данных компаний\n")

        except Exception as e:
            logger.error(f"Ошибка при экспорте в Markdown: {e}")

    def _generate_statistics(self, df: pd.DataFrame) -> Dict[str, List]:
        """Генерация статистики для отчета"""
        stats = {
            'Метрика': [],
            'Значение': [],
            'Описание': []
        }

        # Базовые метрики
        stats['Метрика'].append('Всего компаний')
        stats['Значение'].append(len(df))
        stats['Описание'].append('Общее количество компаний в датасете')

        # Качество данных
        if 'data_quality_score' in df.columns:
            stats['Метрика'].append('Средняя оценка качества')
            stats['Значение'].append(f"{df['data_quality_score'].mean():.1f}/100")
            stats['Описание'].append('Средняя оценка качества данных по всем компаниям')

            stats['Метрика'].append('Компаний с оценкой > 70')
            stats['Значение'].append(len(df[df['data_quality_score'] > 70]))
            stats['Описание'].append('Количество компаний с высоким качеством данных')

        # Поддержка
        if 'has_support_team' in df.columns:
            stats['Метрика'].append('Компании с командой поддержки')
            stats['Значение'].append(
                f"{df['has_support_team'].sum()} ({df['has_support_team'].sum() / len(df) * 100:.1f}%)")
            stats['Описание'].append('Доля компаний, имеющих информацию о команде поддержки')

        if 'support_team_size' in df.columns:
            avg_team = df[df['support_team_size'] > 0]['support_team_size'].mean()
            if not pd.isna(avg_team):
                stats['Метрика'].append('Средний размер команды поддержки')
                stats['Значение'].append(f"{avg_team:.1f} человек")
                stats['Описание'].append('Средний размер команды поддержки среди компаний, где он указан')

        return stats


def main():
    """Основная функция для экспорта данных"""
    print("=" * 60)
    print("🚀 ЗАПУСК ЭКСПОРТА ДАННЫХ КОМПАНИЙ")
    print("=" * 60)

    # Создаем экспортер
    exporter = CompanyDataExporter()

    # Загружаем данные
    df = exporter.load_data()

    if df is None:
        print("❌ Не удалось загрузить данные")
        return

    print(f"📊 Загружено {len(df)} компаний")
    print(f"📋 Колонки: {', '.join(df.columns[:10])}" +
          ("..." if len(df.columns) > 10 else ""))

    # Меню экспорта
    print("\n📁 ВЫБЕРИТЕ ТИП ЭКСПОРТА:")
    print("1. 📤 Экспорт во все форматы")
    print("2. 🎯 Экспорт по качеству данных")
    print("3. 🔍 Аналитический отчет")
    print("4. ⚙️  Фильтрованный экспорт")
    print("5. 📊 Все опции")
    print("0. ❌ Выход")

    try:
        choice = input("\nВведите номер: ").strip()

        if choice == '1':
            # Экспорт во все форматы
            export_name = input("Введите имя для экспорта (по умолчанию 'companies'): ").strip()
            if not export_name:
                export_name = 'companies'

            paths = exporter.export_to_formats(df, export_name)
            print(f"\n✅ Экспортировано в {len(paths)} форматов:")
            for format_name, path in paths.items():
                print(f"   📄 {format_name}: {path}")

        elif choice == '2':
            # Экспорт по качеству
            min_score = input("Минимальная оценка (0-100, по умолчанию 0): ").strip()
            max_score = input("Максимальная оценка (0-100, по умолчанию 100): ").strip()

            min_score = int(min_score) if min_score else 0
            max_score = int(max_score) if max_score else 100

            results = exporter.export_by_quality(df, min_score, max_score)
            print(f"\n✅ Экспортировано {len(results)} категорий качества")

        elif choice == '3':
            # Аналитический отчет
            report_name = input("Введите имя отчета (по умолчанию 'analysis'): ").strip()
            if not report_name:
                report_name = 'analysis'

            report_path = exporter.export_analysis_report(df, report_name)
            print(f"\n✅ Аналитический отчет создан: {report_path}")

        elif choice == '4':
            # Фильтрованный экспорт
            print("\n⚙️  Настройка фильтров:")
            print("Доступные колонки для фильтрации:")
            for i, col in enumerate(df.columns[:15], 1):
                print(f"  {i:2d}. {col}")

            if len(df.columns) > 15:
                print(f"  ... и еще {len(df.columns) - 15} колонок")

            filter_col = input("\nВведите название колонки для фильтрации: ").strip()

            if filter_col in df.columns:
                print(f"\nУникальные значения в '{filter_col}':")
                unique_vals = df[filter_col].dropna().unique()
                for val in unique_vals[:10]:
                    print(f"  - {val}")

                if len(unique_vals) > 10:
                    print(f"  ... и еще {len(unique_vals) - 10} значений")

                filter_val = input(f"\nВведите значение для фильтрации '{filter_col}': ").strip()

                try:
                    # Пробуем преобразовать в число если возможно
                    if df[filter_col].dtype in [np.int64, np.float64]:
                        filter_val = float(filter_val)
                except:
                    pass

                filtered = exporter.export_filtered(
                    df,
                    {filter_col: filter_val},
                    f"filtered_by_{filter_col}"
                )

                if filtered is not None:
                    print(f"\n✅ Экспортировано {len(filtered)} записей")

        elif choice == '5':
            # Все опции
            print("\n🚀 Запуск всех вариантов экспорта...")

            # 1. Все форматы
            paths = exporter.export_to_formats(df, 'complete_export')
            print(f"✅ Все форматы: {len(paths)} файлов")

            # 2. По качеству
            results = exporter.export_by_quality(df)
            print(f"✅ По качеству: {len(results)} категорий")

            # 3. Аналитический отчет
            report_path = exporter.export_analysis_report(df, 'full_analysis')
            print(f"✅ Аналитический отчет: {report_path}")

            print("\n📁 Все экспорты сохранены в папке 'exports/'")

        elif choice == '0':
            print("👋 Выход...")
            return

        else:
            print("❌ Неверный выбор")

        print("\n" + "=" * 60)
        print("✅ ЭКСПОРТ ЗАВЕРШЕН!")
        print("=" * 60)

    except Exception as e:
        print(f"❌ Ошибка: {e}")


if __name__ == "__main__":
    main()