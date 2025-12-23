import pandas as pd
import numpy as np
import re
from datetime import datetime
import os
from typing import Dict, Any, Optional, List
import json


def load_data() -> Optional[Dict[str, pd.DataFrame]]:
    """Загрузка всех указанных файлов"""
    data_sources = {}

    try:
        print("📁 Загрузка данных из всех источников...")

        files_to_load = [
            ('seed', 'data/raw/companies_seed.csv'),
            ('enriched', 'data/raw/enriched_companies_improved_20251223_165111.csv'),
            ('jobs_detailed', 'data/raw/jobs_detailed_20251223_1656.csv'),
            ('jobs_simple', 'data/raw/jobs_simplified.csv')
        ]

        for source_name, file_path in files_to_load:
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path)
                    data_sources[source_name] = df
                    print(f"✅ {source_name}: {len(df)} записей")
                    print(f"   Колонки ({len(df.columns)}): {', '.join(df.columns[:10])}" +
                          ("..." if len(df.columns) > 10 else ""))
                except Exception as e:
                    print(f"❌ Ошибка при чтении {file_path}: {e}")
            else:
                print(f"⚠️  Файл не найден: {file_path}")

        if not data_sources:
            print("❌ Нет данных для обработки")
            return None

        return data_sources

    except Exception as e:
        print(f"❌ Критическая ошибка при загрузке данных: {e}")
        return None


def extract_all_company_info(row: pd.Series, source: str) -> Dict[str, Any]:
    """Извлечение ВСЕЙ информации о компании из строки"""
    info = {'source': source}

    # 1. Основные данные
    name_candidates = ['name', 'company_name', 'employer', 'hh_employer_name']
    for col in name_candidates:
        if col in row and pd.notna(row[col]):
            info['name'] = str(row[col]).strip()
            break

    # 2. Контактные данные
    # Сайты
    site_candidates = ['site', 'site_url', 'website', 'final_url', 'url', 'hh_employer_url', 'support_url', 'kb_url']
    sites = []
    for col in site_candidates:
        if col in row and pd.notna(row[col]):
            site = str(row[col]).strip()
            if site and site not in sites:
                sites.append(site)
    info['sites'] = sites

    # Email - ищем во всех возможных колонках
    email_candidates = ['support_email', 'email', 'e-mail', 'contact_email']
    emails = []
    for col in email_candidates:
        if col in row and pd.notna(row[col]):
            email_val = str(row[col]).strip()
            if '@' in email_val and email_val not in emails:
                emails.append(email_val)
    info['emails'] = emails

    # 3. Бизнес информация
    if 'industry' in row and pd.notna(row['industry']):
        info['industry'] = str(row['industry']).strip()

    if 'inn' in row and pd.notna(row['inn']):
        info['inn'] = str(row['inn']).strip()

    # 4. Support информация (самая важная часть!)
    support_info = {}

    # Размер команды поддержки
    size_cols = ['support_team_size_min', 'support_team_size', 'team_size']
    for col in size_cols:
        if col in row and pd.notna(row[col]):
            try:
                support_info['team_size'] = int(float(row[col]))
            except:
                support_info['team_size'] = str(row[col]).strip()
            break

    # Наличие поддержки
    evidence_cols = ['support_evidence', 'evidence_type']
    for col in evidence_cols:
        if col in row and pd.notna(row[col]):
            support_info['evidence'] = str(row[col]).strip()
            break

    # URL доказательств
    if 'evidence_url' in row and pd.notna(row['evidence_url']):
        support_info['evidence_url'] = str(row['evidence_url']).strip()

    # Каналы поддержки
    channels = {}
    if 'has_support_email' in row and pd.notna(row['has_support_email']):
        channels['email'] = bool(row['has_support_email'])
    if 'has_contact_form' in row and pd.notna(row['has_contact_form']):
        channels['contact_form'] = bool(row['has_contact_form'])
    if 'has_online_chat' in row and pd.notna(row['has_online_chat']):
        channels['online_chat'] = bool(row['has_online_chat'])
    if 'has_messengers' in row and pd.notna(row['has_messengers']):
        channels['messengers'] = bool(row['has_messengers'])
    if 'has_support_section' in row and pd.notna(row['has_support_section']):
        channels['support_section'] = bool(row['has_support_section'])
    if 'has_kb_or_faq' in row and pd.notna(row['has_kb_or_faq']):
        channels['kb_faq'] = bool(row['has_kb_or_faq'])
    if 'mentions_24_7' in row and pd.notna(row['mentions_24_7']):
        channels['24_7'] = bool(row['mentions_24_7'])

    if channels:
        support_info['channels'] = channels

    # Вендор чата
    if 'chat_vendor' in row and pd.notna(row['chat_vendor']):
        support_info['chat_vendor'] = str(row['chat_vendor']).strip()

    # Вакансии поддержки
    if 'support_vacancies_found' in row and pd.notna(row['support_vacancies_found']):
        try:
            support_info['support_vacancies'] = int(float(row['support_vacancies_found']))
        except:
            support_info['support_vacancies'] = str(row['support_vacancies_found']).strip()

    # Детали вакансий
    if 'vacancy_details' in row and pd.notna(row['vacancy_details']):
        support_info['vacancy_details'] = str(row['vacancy_details']).strip()

    if 'vacancies_count' in row and pd.notna(row['vacancies_count']):
        try:
            support_info['total_vacancies'] = int(float(row['vacancies_count']))
        except:
            support_info['total_vacancies'] = str(row['vacancies_count']).strip()

    if support_info:
        info['support_info'] = support_info

    # 5. Анализ и метаданные
    meta = {}
    if 'parsing_success' in row and pd.notna(row['parsing_success']):
        meta['parsing_success'] = bool(row['parsing_success'])
    if 'parsing_method' in row and pd.notna(row['parsing_method']):
        meta['parsing_method'] = str(row['parsing_method']).strip()
    if 'analysis_success' in row and pd.notna(row['analysis_success']):
        meta['analysis_success'] = bool(row['analysis_success'])
    if 'source' in row and pd.notna(row['source']):
        meta['data_source'] = str(row['source']).strip()
    if 'page_title' in row and pd.notna(row['page_title']):
        meta['page_title'] = str(row['page_title']).strip()

    if meta:
        info['metadata'] = meta

    return info


def normalize_company_name(name: str) -> str:
    """Нормализация названия компании"""
    if not name or pd.isna(name):
        return ""

    name = str(name).strip()

    # Удаление лишних пробелов
    name = re.sub(r'\s+', ' ', name)

    # Приведение к единому регистру (но сохраняем оригинал)
    normalized = name.upper()

    # Удаление пунктуации для сравнения
    normalized = re.sub(r'[«»"\'()\[\]!?.,;:]', '', normalized)

    return normalized.strip()


def normalize_url(url: str) -> str:
    """Нормализация URL"""
    if not url or pd.isna(url):
        return ""

    url = str(url).strip().lower()

    # Добавление протокола если нет
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    # Удаление параметров для сравнения
    url = re.sub(r'\?.*$', '', url)

    # Удаление слешей в конце
    url = re.sub(r'/$', '', url)

    return url


def calculate_company_score(company: Dict[str, Any]) -> int:
    """Расчет комплексной оценки качества данных о компании"""
    score = 0

    # Базовые данные (30 баллов)
    if company.get('name'):
        score += 10
    if company.get('inn'):
        score += 10
    if company.get('industry'):
        score += 10

    # Контактные данные (30 баллов)
    if company.get('primary_site'):
        score += 15
    if company.get('primary_email'):
        score += 15

    # Support информация (40 баллов)
    support_info = company.get('support_info', {})
    if support_info:
        score += 10  # За наличие любой support информации

        if support_info.get('team_size'):
            score += 10
        if support_info.get('evidence'):
            score += 10
        if support_info.get('channels'):
            channels = support_info['channels']
            # За каждый канал поддержки
            channel_count = sum(1 for v in channels.values() if v)
            score += min(channel_count * 3, 10)

    return min(score, 100)


def merge_company_info(existing: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    """Объединение информации о компании из разных источников"""
    merged = existing.copy() if existing else {}

    # Обновляем имя если его нет
    if not merged.get('name') and new.get('name'):
        merged['name'] = new['name']

    # Объединяем сайты
    existing_sites = merged.get('sites', [])
    new_sites = new.get('sites', [])
    all_sites = list(set(existing_sites + new_sites))
    if all_sites:
        merged['sites'] = all_sites
        merged['primary_site'] = all_sites[0]  # Первый сайт как основной

    # Объединяем email
    existing_emails = merged.get('emails', [])
    new_emails = new.get('emails', [])
    all_emails = list(set(existing_emails + new_emails))
    if all_emails:
        merged['emails'] = all_emails
        merged['primary_email'] = all_emails[0]  # Первый email как основной

    # Обновляем бизнес информацию
    for field in ['industry', 'inn']:
        if field in new and new[field] and not merged.get(field):
            merged[field] = new[field]

    # Объединяем support информацию
    existing_support = merged.get('support_info', {})
    new_support = new.get('support_info', {})

    if existing_support or new_support:
        merged_support = existing_support.copy()

        # Обновляем числовые поля (берем максимальное значение)
        for field in ['team_size', 'support_vacancies', 'total_vacancies']:
            if field in new_support:
                new_val = new_support[field]
                if field in merged_support:
                    try:
                        # Берем максимальное значение
                        if isinstance(new_val, (int, float)) and isinstance(merged_support[field], (int, float)):
                            merged_support[field] = max(merged_support[field], new_val)
                        else:
                            merged_support[field] = new_val
                    except:
                        merged_support[field] = new_val
                else:
                    merged_support[field] = new_val

        # Объединяем каналы поддержки
        if 'channels' in new_support:
            if 'channels' not in merged_support:
                merged_support['channels'] = {}
            for channel, value in new_support['channels'].items():
                if channel not in merged_support['channels'] or not merged_support['channels'][channel]:
                    merged_support['channels'][channel] = value

        # Обновляем текстовые поля
        for field in ['evidence', 'evidence_url', 'chat_vendor', 'vacancy_details']:
            if field in new_support and new_support[field] and not merged_support.get(field):
                merged_support[field] = new_support[field]

        merged['support_info'] = merged_support

    # Объединяем метаданные
    existing_meta = merged.get('metadata', {})
    new_meta = new.get('metadata', {})

    if existing_meta or new_meta:
        merged_meta = existing_meta.copy()
        for key, value in new_meta.items():
            if key not in merged_meta:
                merged_meta[key] = value
        merged['metadata'] = merged_meta

    # Обновляем источники
    if 'sources' not in merged:
        merged['sources'] = []

    if new.get('source') and new['source'] not in merged['sources']:
        merged['sources'].append(new['source'])

    # Добавляем timestamp
    merged['last_updated'] = datetime.now().isoformat()

    return merged


def create_master_dataset(data_sources: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Создание мастер-датасета из всех источников"""
    print("\n🔗 Создание мастер-датасета...")

    master_companies = {}  # normalized_name -> company_data

    # Обрабатываем каждый источник
    for source_name, df in data_sources.items():
        print(f"   📊 Обработка {source_name} ({len(df)} записей)...")

        for idx, row in df.iterrows():
            # Извлекаем всю информацию
            company_info = extract_all_company_info(row, source_name)

            if not company_info.get('name'):
                continue

            # Нормализуем имя для группировки
            normalized_name = normalize_company_name(company_info['name'])

            if normalized_name in master_companies:
                # Объединяем с существующей записью
                master_companies[normalized_name] = merge_company_info(
                    master_companies[normalized_name],
                    company_info
                )
            else:
                # Создаем новую запись
                company_info['normalized_name'] = normalized_name
                master_companies[normalized_name] = company_info

    print(f"   ✅ Объединено {len(master_companies)} уникальных компаний")
    return master_companies


def enhance_and_score_companies(master_companies: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Улучшение данных и расчет итоговых оценок"""
    print("   🎯 Улучшение данных и расчет оценок...")

    enhanced_companies = []

    for normalized_name, company in master_companies.items():
        # Генерируем уникальный ID
        company['company_id'] = f"C{len(enhanced_companies) + 1:04d}"

        # Рассчитываем оценки
        company['data_quality_score'] = calculate_company_score(company)

        # Дополнительные метрики
        support_info = company.get('support_info', {})

        # Наличие поддержки
        company['has_support_team'] = bool(support_info.get('team_size'))
        company['support_team_size'] = support_info.get('team_size', 0)

        # Количество каналов поддержки
        channels = support_info.get('channels', {})
        company['support_channels_count'] = sum(1 for v in channels.values() if v)
        company['has_24_7_support'] = channels.get('24_7', False)

        # Информация о вакансиях
        company['support_vacancies'] = support_info.get('support_vacancies', 0)
        company['total_vacancies'] = support_info.get('total_vacancies', 0)

        # Собираем все источники
        company['data_sources'] = ', '.join(company.get('sources', []))

        # Создаем чистое описание
        description_parts = []
        if company.get('industry'):
            description_parts.append(f"Отрасль: {company['industry']}")
        if company.get('support_team_size'):
            description_parts.append(f"Размер команды поддержки: {company['support_team_size']}")
        if company.get('support_channels_count', 0) > 0:
            description_parts.append(f"Каналы поддержки: {company['support_channels_count']}")

        company['description'] = ' | '.join(description_parts) if description_parts else "Информация отсутствует"

        enhanced_companies.append(company)

    # Сортируем по оценке качества
    enhanced_companies.sort(key=lambda x: x['data_quality_score'], reverse=True)

    print(f"   ✅ Улучшено {len(enhanced_companies)} компаний")
    return enhanced_companies


def save_enhanced_results(companies: List[Dict[str, Any]]):
    """Сохранение улучшенных результатов"""
    if not companies:
        print("❌ Нет данных для сохранения")
        return

    os.makedirs('data/processed', exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Создаем DataFrame для сохранения
    df = pd.DataFrame(companies)

    # Определяем порядок колонок для лучшей читаемости
    core_columns = [
        'company_id', 'name', 'normalized_name', 'industry', 'inn',
        'primary_site', 'primary_email', 'data_quality_score'
    ]

    support_columns = [
        'has_support_team', 'support_team_size', 'support_channels_count',
        'has_24_7_support', 'support_vacancies', 'total_vacancies'
    ]

    # Собираем все колонки в правильном порядке
    all_columns = []

    # Основные колонки (всегда первые)
    for col in core_columns:
        if col in df.columns:
            all_columns.append(col)

    # Support колонки
    for col in support_columns:
        if col in df.columns:
            all_columns.append(col)

    # Остальные колонки в алфавитном порядке
    other_columns = sorted([col for col in df.columns if col not in all_columns])
    all_columns.extend(other_columns)

    # Реорганизуем DataFrame
    df = df[all_columns]

    # Сохраняем в разных форматах
    # 1. Основной файл (CSV)
    main_filename = f'companies_master_dataset_{timestamp}.csv'
    main_path = f'data/processed/{main_filename}'
    df.to_csv(main_path, index=False, encoding='utf-8-sig')

    # 2. Упрощенная версия для анализа
    simple_cols = [
        'company_id', 'name', 'industry', 'primary_site',
        'data_quality_score', 'support_team_size', 'support_channels_count',
        'has_24_7_support', 'data_sources'
    ]
    simple_df = df[[col for col in simple_cols if col in df.columns]]
    simple_filename = f'companies_analysis_view_{timestamp}.csv'
    simple_path = f'data/processed/{simple_filename}'
    simple_df.to_csv(simple_path, index=False, encoding='utf-8-sig')

    # 3. Excel с форматированием
    excel_filename = f'companies_dashboard_{timestamp}.xlsx'
    excel_path = f'data/processed/{excel_filename}'

    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        # Лист с полными данными
        df.to_excel(writer, sheet_name='Полные данные', index=False)

        # Лист с анализом
        analysis_df = df.nlargest(50, 'data_quality_score')
        analysis_df.to_excel(writer, sheet_name='Топ-50 компаний', index=False)

        # Лист со статистикой
        stats_data = {
            'Метрика': [
                'Всего компаний',
                'Средняя оценка качества',
                'Компании с командой поддержки',
                'Средний размер команды поддержки',
                'Компании с 24/7 поддержкой',
                'Среднее количество каналов поддержки'
            ],
            'Значение': [
                len(df),
                f"{df['data_quality_score'].mean():.1f}/100",
                f"{df['has_support_team'].sum()} ({df['has_support_team'].sum() / len(df) * 100:.1f}%)",
                f"{df['support_team_size'].mean():.1f}",
                f"{df['has_24_7_support'].sum()} ({df['has_24_7_support'].sum() / len(df) * 100:.1f}%)",
                f"{df['support_channels_count'].mean():.1f}"
            ]
        }
        stats_df = pd.DataFrame(stats_data)
        stats_df.to_excel(writer, sheet_name='Статистика', index=False)

    print(f"\n💾 РЕЗУЛЬТАТЫ СОХРАНЕНЫ:")
    print(f"   📊 Основной файл: {main_path}")
    print(f"   📈 Для анализа: {simple_path}")
    print(f"   📋 Excel дашборд: {excel_path}")

    return df


def print_detailed_analysis(df: pd.DataFrame):
    """Печать детального анализа результатов"""
    print("\n" + "=" * 80)
    print("📊 ДЕТАЛЬНЫЙ АНАЛИЗ РЕЗУЛЬТАТОВ")
    print("=" * 80)

    # Общая статистика
    print(f"\n📈 ОБЩАЯ СТАТИСТИКА:")
    print(f"   Всего компаний: {len(df)}")
    print(f"   Уникальных названий: {df['normalized_name'].nunique()}")

    # Распределение по оценкам
    print(f"\n🏆 РАСПРЕДЕЛЕНИЕ ПО ОЦЕНКАМ КАЧЕСТВА:")
    bins = [0, 30, 50, 70, 90, 100]
    labels = ['Очень низкое', 'Низкое', 'Среднее', 'Высокое', 'Очень высокое']

    df['quality_category'] = pd.cut(df['data_quality_score'], bins=bins, labels=labels, right=False)
    quality_dist = df['quality_category'].value_counts().sort_index()

    for category, count in quality_dist.items():
        percentage = count / len(df) * 100
        print(f"   {category}: {count} компаний ({percentage:.1f}%)")

    # Анализ поддержки
    print(f"\n🛡️  АНАЛИЗ ПОДДЕРЖКИ:")
    print(
        f"   Компании с командой поддержки: {df['has_support_team'].sum()} ({df['has_support_team'].sum() / len(df) * 100:.1f}%)")
    print(
        f"   Компании с 24/7 поддержкой: {df['has_24_7_support'].sum()} ({df['has_24_7_support'].sum() / len(df) * 100:.1f}%)")

    if 'support_team_size' in df.columns:
        avg_team_size = df[df['support_team_size'] > 0]['support_team_size'].mean()
        print(f"   Средний размер команды поддержки: {avg_team_size:.1f} человек")

    if 'support_channels_count' in df.columns:
        avg_channels = df['support_channels_count'].mean()
        print(f"   Среднее количество каналов поддержки: {avg_channels:.1f}")

    # Топ компаний
    print(f"\n🏅 ТОП-10 КОМПАНИЙ ПО КАЧЕСТВУ ДАННЫХ:")
    top_10 = df.nlargest(10, 'data_quality_score')

    for idx, row in top_10.iterrows():
        score = row['data_quality_score']
        name = row.get('name', '')[:35]
        industry = row.get('industry', '')[:20]
        team_size = row.get('support_team_size', 0)
        channels = row.get('support_channels_count', 0)

        print(f"   {score:3.0f}/100 | {name:35} | {industry:20} | Команда: {team_size:2d} | Каналы: {channels}")

    # Анализ по отраслям
    if 'industry' in df.columns:
        print(f"\n🏭 АНАЛИЗ ПО ОТРАСЛЯМ:")
        industry_stats = df.groupby('industry').agg({
            'data_quality_score': 'mean',
            'has_support_team': 'sum',
            'support_team_size': 'mean'
        }).round(1)

        industry_stats = industry_stats.sort_values('data_quality_score', ascending=False).head(10)

        for industry, stats in industry_stats.iterrows():
            avg_score = stats['data_quality_score']
            support_count = stats['has_support_team']
            avg_team = stats['support_team_size']

            print(
                f"   {industry[:30]:30} | Оценка: {avg_score:.0f}/100 | С поддержкой: {support_count} | Ср. команда: {avg_team:.0f}")

    # Рекомендации по улучшению
    print(f"\n💡 РЕКОМЕНДАЦИИ ДЛЯ УЛУЧШЕНИЯ:")
    low_quality = df[df['data_quality_score'] < 50]
    if len(low_quality) > 0:
        print(f"   • {len(low_quality)} компаний имеют оценку ниже 50 - требуют дополнительного сбора данных")

    no_support = df[~df['has_support_team']]
    if len(no_support) > 0:
        print(f"   • {len(no_support)} компаний не имеют информации о команде поддержки")

    print(f"   • Рекомендуется провести дополнительный парсинг сайтов для получения email и телефонов")


def main():
    """Основная функция"""
    print("=" * 80)
    print("🚀 ЗАПУСК ПРОФЕССИОНАЛЬНОЙ ОБРАБОТКИ ДАННЫХ КОМПАНИЙ")
    print("=" * 80)

    # Загрузка данных
    data_sources = load_data()

    if not data_sources:
        print("❌ Не удалось загрузить данные")
        return

    # Создание мастер-датасета
    master_companies = create_master_dataset(data_sources)

    if not master_companies:
        print("❌ Не удалось создать мастер-датасет")
        return

    # Улучшение и оценка данных
    enhanced_companies = enhance_and_score_companies(master_companies)

    # Сохранение результатов
    result_df = save_enhanced_results(enhanced_companies)

    # Детальный анализ
    print_detailed_analysis(result_df)

    print("\n" + "=" * 80)
    print("✅ ПРОЦЕСС УСПЕШНО ЗАВЕРШЕН!")
    print("=" * 80)


if __name__ == "__main__":
    main()