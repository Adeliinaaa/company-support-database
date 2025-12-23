# Анализ вакансий компаний через HH API

import requests
import pandas as pd
import time
import re
import os
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from urllib.parse import quote_plus


def load_companies() -> pd.DataFrame:
    """Загружаем список компаний"""
    possible_paths = [
        'data/raw/companies_seed.csv',
        '../data/raw/companies_seed.csv',
        os.path.join(os.path.dirname(__file__), '../data/raw/companies_seed.csv'),
        'companies_seed.csv'
    ]

    for path in possible_paths:
        if os.path.exists(path):
            print(f"📁 Загружаю компании из: {path}")
            df = pd.read_csv(path, encoding='utf-8-sig')
            print(f"✅ Загружено {len(df)} компаний")
            return df

    print("❌ Файл с компаниями не найден")
    return pd.DataFrame()


def find_company_variants(company_name: str) -> List[str]:
    """
    Создаем варианты названий для поиска на HH
    Пример: "Сбербанк" → ["Сбербанк", "Сбер", "ПАО Сбербанк"]
    """
    variants = [company_name]

    # Распространенные сокращения и варианты
    name_mappings = {
        'Сбербанк': ['Сбер', 'Сбербанк России', 'ПАО Сбербанк'],
        'Тинькофф': ['Тинькофф Банк', 'Тинькофф банк'],
        'Альфа-Банк': ['Альфабанк', 'Альфа Банк'],
        'ВТБ': ['Банк ВТБ', 'ВТБ банк'],
        'МТС': ['МТС Банк', 'МТС банк'],
        'Яндекс': ['Яндекс.Такси', 'Яндекс Еда', 'Яндекс.Маркет'],
        'OZON': ['Ozon', 'Озон'],
        'Wildberries': ['Вайлдберриз', 'WB'],
        'DNS': ['ДНС', 'DNS-Shop'],
        'М.Видео': ['МВидео', 'М. Видео'],
        'Эльдорадо': ['Eldorado'],
        'РЖД': ['Российские железные дороги'],
        'Газпром': ['Газпромбанк', 'Газпром нефть'],
        'Лукойл': ['ЛУКОЙЛ'],
        'Wargaming': ['Wargaming.net'],
    }

    # Добавляем известные варианты
    for key, values in name_mappings.items():
        if key.lower() in company_name.lower():
            variants.extend(values)

    # Убираем дубли
    return list(set(variants))[:5]  # Максимум 5 вариантов


def smart_search_company_on_hh(company_name: str) -> Optional[Dict]:
    """
    Умный поиск компании на HH.ru с несколькими вариантами названий
    """
    variants = find_company_variants(company_name)

    headers = {
        'User-Agent': 'CompanySupportAnalyzer/1.0',
        'HH-User-Agent': 'CompanySupportAnalyzer/1.0'
    }

    for variant in variants:
        try:
            url = f"https://api.hh.ru/employers"
            params = {
                'text': variant,
                'area': 113,  # Россия
                'per_page': 3,
                'only_with_vacancies': True  # Только компании с вакансиями
            }

            response = requests.get(url, params=params, headers=headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])

                if items:
                    # Выбираем наиболее релевантный результат
                    employer = items[0]

                    # Проверяем схожесть названий
                    hh_name = employer.get('name', '').lower()
                    our_name = company_name.lower()

                    # Если названия достаточно похожи
                    if (hh_name in our_name or our_name in hh_name or
                            any(word in hh_name for word in our_name.split())):
                        print(f"   ✅ Нашли: {employer.get('name')} (по варианту: '{variant}')")

                        return {
                            'id': employer.get('id'),
                            'name': employer.get('name'),
                            'url': employer.get('alternate_url'),
                            'site_url': employer.get('site_url'),
                            'trusted': employer.get('trusted', False),
                            'open_vacancies': employer.get('open_vacancies', 0)
                        }

            time.sleep(0.3)  # Пауза между запросами

        except Exception as e:
            print(f"   ⚠️  Ошибка поиска варианта '{variant}': {e}")
            continue

    return None


def search_all_support_vacancies(employer_id: str, company_name: str) -> List[Dict]:
    """
    Поиск ВСЕХ вакансий поддержки (не только первой страницы)
    """
    all_vacancies = []

    # Расширенный список ключевых слов
    support_keywords = [
        # Русские
        'поддержк', 'оператор', 'контакт-центр', 'контакт центр',
        'call center', 'колл-центр', 'колл центр',
        'техподдержк', 'тех поддержк', 'service desk',
        'специалист поддержки', 'менеджер поддержки',
        'клиентск', 'клиентский', 'customer',
        'helpdesk', 'help desk', 'хелпдеск',
        'сервисный инженер', 'сервис инженер',
        'модератор', 'модерация',
        'консультант', 'консультирование',

        # Английские
        'support', 'customer support', 'tech support',
        'customer service', 'client service',
        'service engineer', 'support engineer',
        'contact center', 'callcentre',
        'help desk', 'service desk'
    ]

    try:
        # Проверяем сколько всего вакансий у работодателя
        url = f"https://api.hh.ru/vacancies"
        params = {
            'employer_id': employer_id,
            'area': 113,
            'per_page': 1,  # Только для подсчета
            'page': 0
        }

        headers = {'User-Agent': 'CompanySupportAnalyzer/1.0'}
        response = requests.get(url, params=params, headers=headers, timeout=10)

        if response.status_code == 200:
            data = response.json()
            total_found = data.get('found', 0)
            pages = min(data.get('pages', 1), 3)  # Максимум 3 страницы

            print(f"   📊 Всего вакансий у работодателя: {total_found}")

            # Собираем вакансии со всех страниц (но ограничимся разумным количеством)
            for page in range(pages):
                params = {
                    'employer_id': employer_id,
                    'area': 113,
                    'per_page': 100,
                    'page': page
                }

                response = requests.get(url, params=params, headers=headers, timeout=10)

                if response.status_code == 200:
                    page_data = response.json()

                    for vacancy in page_data.get('items', []):
                        vacancy_name = vacancy.get('name', '').lower()
                        snippet = vacancy.get('snippet', {})
                        requirement = snippet.get('requirement', '').lower()
                        responsibility = snippet.get('responsibility', '').lower()

                        # Объединяем текст для поиска
                        full_text = f"{vacancy_name} {requirement} {responsibility}"

                        # Проверяем, что это вакансия поддержки
                        is_support_vacancy = any(
                            keyword in full_text for keyword in support_keywords
                        )

                        if is_support_vacancy:
                            # Извлекаем информацию о графике
                            schedule = vacancy.get('schedule', {}).get('name', '')
                            schedule_lower = schedule.lower()

                            # Определяем тип графика
                            has_shifts = any(
                                word in schedule_lower for word in ['сменный', '2/2', '3/3', 'ночн', '24/7'])
                            is_fulltime = 'полный день' in schedule_lower or 'full day' in schedule_lower

                            all_vacancies.append({
                                'id': vacancy.get('id'),
                                'name': vacancy.get('name'),
                                'url': vacancy.get('alternate_url'),
                                'published_at': vacancy.get('published_at'),
                                'experience': vacancy.get('experience', {}).get('name', ''),
                                'employment': vacancy.get('employment', {}).get('name', ''),
                                'schedule': schedule,
                                'has_shifts': has_shifts,
                                'is_fulltime': is_fulltime,
                                'salary': vacancy.get('salary'),
                                'description': full_text[:300],  # Сохраняем часть описания
                                'vacancy_type': 'support'
                            })

                time.sleep(0.3)  # Пауза между страницами

                # Если уже много вакансий, можно остановиться
                if len(all_vacancies) >= 15:
                    break

        print(f"   📊 Найдено вакансий поддержки: {len(all_vacancies)}")

    except Exception as e:
        print(f"   ⚠️  Ошибка поиска вакансий: {e}")

    return all_vacancies


def create_evidence_from_vacancies(vacancies: List[Dict], company_name: str) -> List[Dict]:
    """
    Создаем отдельные доказательства из каждой вакансии
    """
    evidences = []

    for i, vac in enumerate(vacancies):
        evidence_id = f"{company_name}_vacancy_{i + 1}"

        # Базовое доказательство - 1 вакансия = минимум 1 человек
        base_size = 1

        # Увеличиваем оценку если:
        # 1. Сменный график = минимум 4 человека на позицию
        if vac.get('has_shifts'):
            base_size = 4

        # 2. Полная занятость = 1 человек
        elif vac.get('is_fulltime'):
            base_size = 1

        # 3. Руководящая позиция = команда подчиненных
        position_lower = vac['name'].lower()
        if any(word in position_lower for word in ['руководитель', 'lead', 'head', 'менеджер', 'управляющий']):
            base_size = 3  # Руководитель + минимум 2 подчиненных

        # Формируем текстовое доказательство
        evidence_text = f"Вакансия: '{vac['name']}'"

        if vac.get('has_shifts'):
            evidence_text += f" (сменный график: {vac['schedule']})"

        if vac.get('experience'):
            evidence_text += f", требуется опыт: {vac['experience']}"

        # Создаем запись доказательства
        evidence = {
            'evidence_id': evidence_id,
            'vacancy_name': vac['name'],
            'vacancy_url': vac['url'],
            'schedule': vac.get('schedule', ''),
            'has_shifts': vac.get('has_shifts', False),
            'estimated_team_size': base_size,
            'evidence_text': evidence_text,
            'evidence_type': 'vacancy',
            'published_date': vac.get('published_at', '')
        }

        evidences.append(evidence)

    return evidences


def calculate_team_size_from_evidences(evidences: List[Dict]) -> Dict:
    """
    Рассчитываем общий размер команды на основе всех вакансий
    """
    if not evidences:
        return {
            'support_team_size_min': 0,
            'support_evidence': '',
            'total_vacancies': 0
        }

    # 1. Суммируем оценки от каждой вакансии
    total_estimated = sum(ev['estimated_team_size'] for ev in evidences)

    # 2. Применяем мультипликаторы для группы вакансий
    multiplier = 1.0

    # Если есть сменные вакансии - увеличиваем оценку
    shift_vacancies = [ev for ev in evidences if ev['has_shifts']]
    if len(shift_vacancies) >= 2:
        multiplier = max(multiplier, 1.5)  # Две сменные вакансии = крупнее команда

    # Если много вакансий (>3) - увеличиваем оценку
    if len(evidences) >= 3:
        multiplier = max(multiplier, 1.3)

    # Итоговая оценка
    final_estimate = int(total_estimated * multiplier)

    # Минимум 10 если есть хотя бы 3 вакансии или 2 сменные
    if final_estimate < 10 and (len(evidences) >= 3 or len(shift_vacancies) >= 2):
        final_estimate = 10

    # Формируем текстовое доказательство
    evidence_parts = []

    if len(evidences) > 0:
        evidence_parts.append(f"{len(evidences)} вакансий поддержки")

    if len(shift_vacancies) > 0:
        evidence_parts.append(f"{len(shift_vacancies)} со сменным графиком")

    evidence_text = f"На HH.ru найдено: {', '.join(evidence_parts)}"

    # Добавляем примеры вакансий
    if len(evidences) <= 3:
        vacancy_names = [ev['vacancy_name'] for ev in evidences[:3]]
        evidence_text += f" ({', '.join(vacancy_names)})"

    return {
        'support_team_size_min': final_estimate,
        'support_evidence': evidence_text,
        'total_vacancies': len(evidences),
        'shift_vacancies': len(shift_vacancies),
        'all_evidences': evidences
    }


def analyze_company_with_detailed_vacancies(company: Dict) -> Dict:
    """Детальный анализ с сохранением всех вакансий"""
    company_name = company.get('name', 'Unknown')

    print(f"\n🔍 Анализ: {company_name}")

    result = {
        'name': company_name,
        'site': company.get('site_url', ''),
        'inn': company.get('inn', ''),
        'hh_found': False,
        'hh_employer_name': '',
        'hh_employer_url': '',
        'total_vacancies_found': 0,
        'support_vacancies_found': 0,
        'support_team_size_min': 0,
        'support_evidence': '',
        'evidence_url': '',
        'evidence_type': 'jobs',
        'vacancy_details': [],
        'analysis_success': False,
        'error': ''
    }

    try:
        # 1. Ищем компанию на HH
        print("   🔎 Поиск компании на HH.ru...")
        employer_info = smart_search_company_on_hh(company_name)

        if not employer_info:
            print("   ⚠️  Компания не найдена на HH.ru")
            result['error'] = 'Company not found on HH'
            return result

        result['hh_found'] = True
        result['hh_employer_name'] = employer_info['name']
        result['hh_employer_url'] = employer_info['url']

        # 2. Ищем ВСЕ вакансии поддержки
        print("   🔎 Поиск всех вакансий поддержки...")
        vacancies = search_all_support_vacancies(employer_info['id'], company_name)

        result['total_vacancies_found'] = employer_info.get('open_vacancies', 0)
        result['support_vacancies_found'] = len(vacancies)

        if not vacancies:
            print("   ⚠️  Вакансий поддержки не найдено")
            result['error'] = 'No support vacancies found'
            result['analysis_success'] = True  # Технически успешно, просто нет вакансий
            return result

        # 3. Создаем отдельные доказательства из каждой вакансии
        print("   📝 Создание доказательств из вакансий...")
        evidences = create_evidence_from_vacancies(vacancies, company_name)

        # Сохраняем детали вакансий
        result['vacancy_details'] = [
            {
                'name': ev['vacancy_name'],
                'url': ev['vacancy_url'],
                'schedule': ev['schedule'],
                'estimated_size': ev['estimated_team_size']
            }
            for ev in evidences[:5]  # Сохраняем первые 5
        ]

        # 4. Рассчитываем общий размер команды
        team_calculation = calculate_team_size_from_evidences(evidences)

        result['support_team_size_min'] = team_calculation['support_team_size_min']
        result['support_evidence'] = team_calculation['support_evidence']
        result['evidence_url'] = employer_info['url']  # Или первую вакансию

        # 5. Выводим результаты
        if result['support_team_size_min'] >= 10:
            print(f"   🎯 ДОКАЗАТЕЛЬСТВО: {result['support_team_size_min']}+ человек")
            print(f"   📊 Основание: {result['support_evidence']}")

            # Показываем примеры вакансий
            print(f"   📋 Примеры вакансий:")
            for i, vac in enumerate(result['vacancy_details'][:3], 1):
                print(f"      {i}. {vac['name']} ({vac['schedule']})")

        elif result['support_vacancies_found'] > 0:
            print(f"   📊 Найдено вакансий: {result['support_vacancies_found']}")
            print(f"   ⚠️  Недостаточно для доказательства 10+ (оценка: {result['support_team_size_min']})")

        result['analysis_success'] = True

    except Exception as e:
        print(f"   ❌ Ошибка при анализе: {e}")
        result['error'] = str(e)

    return result


def main():
    """Основная функция"""
    print("=" * 70)
    print("🔍 ДЕТАЛЬНЫЙ АНАЛИЗ ВАКАНСИЙ С СОХРАНЕНИЕМ ВСЕХ ДОКАЗАТЕЛЬСТВ")
    print("=" * 70)

    try:
        # Загружаем компании
        df = load_companies()
        if len(df) == 0:
            print("❌ Нет данных для анализа")
            return

        # Анализируем первые 55 компаний
        companies_to_process = df.head(55).copy()

        print(f"\n📊 Будут проанализированы {len(companies_to_process)} компаний")
        print("⏳ Это займет 10-15 минут...")

        results = []
        success_count = 0
        evidence_count = 0

        for idx, company in companies_to_process.iterrows():
            print(f"\n[{idx + 1}/{len(companies_to_process)}] ", end="")
            result = analyze_company_with_detailed_vacancies(company.to_dict())
            results.append(result)

            if result.get('analysis_success'):
                success_count += 1

            if result.get('support_team_size_min', 0) >= 10:
                evidence_count += 1

        # Сохраняем полные результаты
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        output_file = f"data/raw/jobs_detailed_{timestamp}.csv"

        os.makedirs('data/raw', exist_ok=True)

        # Сохраняем основной DataFrame
        df_results = pd.DataFrame(results)
        df_results.to_csv(output_file, index=False, encoding='utf-8-sig')

        # Также создаем упрощенную версию для объединения
        simplified = []
        for result in results:
            if result.get('analysis_success'):
                simplified.append({
                    'name': result['name'],
                    'site': result['site'],
                    'inn': result['inn'],
                    'support_team_size_min': result.get('support_team_size_min', 0),
                    'support_evidence': result.get('support_evidence', ''),
                    'evidence_url': result.get('evidence_url', ''),
                    'evidence_type': 'jobs',
                    'source': 'hh_api',
                    'hh_employer_url': result.get('hh_employer_url', ''),
                    'vacancies_count': result.get('support_vacancies_found', 0)
                })

        df_simple = pd.DataFrame(simplified)
        df_simple.to_csv('data/raw/jobs_simplified.csv', index=False, encoding='utf-8-sig')

        # Статистика
        print(f"\n{'=' * 70}")
        print("📊 ИТОГИ ДЕТАЛЬНОГО АНАЛИЗА:")
        print(f"{'=' * 70}")

        if results:
            stats = [
                ('Всего компаний', len(results)),
                ('Найдены на HH.ru', sum(1 for r in results if r['hh_found'])),
                ('Успешно проанализированы', success_count),
                ('', ''),
                ('Нашли вакансии поддержки', sum(1 for r in results if r['support_vacancies_found'] > 0)),
                ('С 1-2 вакансиями', sum(1 for r in results if 1 <= r['support_vacancies_found'] <= 2)),
                ('С 3+ вакансиями', sum(1 for r in results if r['support_vacancies_found'] >= 3)),
                ('Со сменным графиком',
                 sum(1 for r in results if any(v.get('has_shifts') for v in r.get('vacancy_details', [])))),
                ('', ''),
                ('С доказательствами 10+', evidence_count),
                ('С доказательствами 15+', sum(1 for r in results if r.get('support_team_size_min', 0) >= 15)),
                ('С доказательствами 20+', sum(1 for r in results if r.get('support_team_size_min', 0) >= 20))
            ]

            for label, value in stats:
                if label == '':
                    print("   " + "-" * 40)
                else:
                    print(f"   {label:35}: {value:3d}")

        # Показываем ТОП компаний с доказательствами
        companies_with_evidence = [r for r in results if r.get('support_team_size_min', 0) >= 10]

        if companies_with_evidence:
            print(f"\n{'=' * 70}")
            print("🎯 ТОП КОМПАНИЙ С ЛУЧШИМИ ДОКАЗАТЕЛЬСТВАМИ:")
            print(f"{'=' * 70}")

            # Сортируем по размеру команды
            companies_with_evidence.sort(key=lambda x: x.get('support_team_size_min', 0), reverse=True)

            for idx, company in enumerate(companies_with_evidence[:10], 1):
                vac_count = company.get('support_vacancies_found', 0)
                team_size = company.get('support_team_size_min', 0)
                evidence = company.get('support_evidence', '')[:60]

                print(f"   {idx:2d}. {company['name'][:25]:25} | {team_size:3d}+ чел.")
                print(f"       📊 {vac_count} вакансий | {evidence}...")

                # Показываем 1-2 примера вакансий
                if company.get('vacancy_details'):
                    for vac in company['vacancy_details'][:2]:
                        print(f"       • {vac['name'][:40]}")
                print()

        print(f"\n💾 Результаты сохранены:")
        print(f"   📁 Детальные: {output_file}")
        print(f"   📁 Упрощенные: data/raw/jobs_simplified.csv")

        print(f"\n✅ Анализ завершен!")
        print(f"✅ Успешно проанализировано: {success_count}/{len(companies_to_process)} компаний")
        print(f"✅ С доказательствами 10+: {evidence_count} компаний")


        return results

    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()