# Парсинг сайтов компаний для поиска признаков поддержки и доказательств 10+ человек

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from typing import Dict, List, Optional
import urllib.parse
from urllib.parse import urljoin
import os


def load_companies_from_csv(filename: str = None) -> pd.DataFrame:
    """
    Загружаем список компаний из CSV файла
    """
    if filename is None:
        # Ищем файл в разных местах
        possible_paths = [
            'data/raw/companies_seed.csv',
            '../data/raw/companies_seed.csv',
            os.path.join(os.path.dirname(__file__), '../data/raw/companies_seed.csv'),
            os.path.join(os.path.expanduser("~"), "Desktop", "companies_seed.csv")
        ]

        for path in possible_paths:
            if os.path.exists(path):
                filename = path
                break

    if not filename or not os.path.exists(filename):
        raise FileNotFoundError(f"Файл с компаниями не найден. Искали: {possible_paths}")

    print(f"📁 Загружаю компании из: {filename}")
    df = pd.read_csv(filename, encoding='utf-8-sig')
    print(f"✅ Загружено {len(df)} компаний")
    return df


def normalize_url(url: str) -> str:
    """
    Нормализация URL
    """
    if not url:
        return ""

    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    return url.rstrip('/')


def safe_request(url: str, max_retries: int = 2) -> Optional[requests.Response]:
    """
    Безопасный запрос к сайту с обработкой ошибок
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=15, verify=False)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"   ⚠️  Попытка {attempt + 1}/{max_retries} для {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            continue
        except Exception as e:
            print(f"   ❌ Неожиданная ошибка для {url}: {e}")
            break

    return None


def find_support_features(html: str, url: str, company_name: str) -> Dict:
    """
    Ищем признаки поддержки на странице
    """
    soup = BeautifulSoup(html, 'html.parser')
    text_lower = soup.get_text().lower()

    # Базовые признаки
    features = {
        'has_support_email': False,
        'has_contact_form': False,
        'has_online_chat': False,
        'has_messengers': False,
        'has_support_section': False,
        'has_kb_or_faq': False,
        'mentions_24_7': False,
        'support_team_size_min': 0,
        'support_evidence': '',
        'evidence_url': '',
        'evidence_type': 'site',
        'support_email': '',
        'support_url': '',
        'kb_url': '',
        'chat_vendor': '',
        'source': 'company_site'
    }

    # 1. Поиск поддержки 24/7 (Уровень B)
    patterns_24_7 = [
        r'24/7', r'24\s*часа', r'круглосуточно', r'круглые\s*сутки',
        r'всегда\s*на\s*связи', r'работаем\s*без\s*выходных',
        r'поддержка\s*24', r'24\s*часа\s*в\s*сутки', r'24x7'
    ]

    for pattern in patterns_24_7:
        if re.search(pattern, text_lower, re.IGNORECASE):
            features['mentions_24_7'] = True
            # Если есть 24/7, предполагаем минимум 10 человек (сменный график)
            features['support_team_size_min'] = 10
            features['support_evidence'] = f"Поддержка 24/7 (сменный график требует минимум 10 человек)"
            features['evidence_url'] = url
            break

    # 2. Поиск email поддержки
    email_patterns = [
        r'support@[\w\.-]+', r'help@[\w\.-]+', r'info@[\w\.-]+',
        r'поддержка@[\w\.-]+', r'помощь@[\w\.-]+'
    ]

    for pattern in email_patterns:
        emails = re.findall(pattern, text_lower)
        for email in emails:
            features['has_support_email'] = True
            features['support_email'] = email
            break

    # 3. Поиск контактной формы
    form_keywords = ['форма обратной связи', 'контактная форма', 'напишите нам',
                     'обратная связь', 'задать вопрос', 'contact form']

    forms = soup.find_all(['form', 'div', 'section'])
    for form in forms:
        form_text = form.get_text().lower()
        if any(keyword in form_text for keyword in form_keywords):
            features['has_contact_form'] = True
            break

    # 4. Поиск онлайн-чата
    chat_indicators = ['чат', 'online chat', 'live chat', 'онлайн-чат',
                       'jivo', 'livechat', 'chatra', 'drift']

    for indicator in chat_indicators:
        if indicator in text_lower:
            features['has_online_chat'] = True
            break

    # 5. Поиск мессенджеров
    messengers = ['telegram', 'whatsapp', 'viber', 'vkontakte', 'vk.com']
    for messenger in messengers:
        if messenger in text_lower:
            features['has_messengers'] = True
            break

    # 6. Поиск раздела поддержки
    support_keywords = ['поддержк', 'помощь', 'help', 'support', 'служба поддержки',
                        'контакт-центр', 'техподдержка', 'customer support']

    links = soup.find_all('a')
    for link in links:
        link_text = link.get_text().lower()
        link_href = link.get('href', '')

        if any(keyword in link_text for keyword in support_keywords):
            features['has_support_section'] = True
            features['support_url'] = urljoin(url, link_href)
            break

    # 7. Поиск FAQ / Базы знаний
    faq_keywords = ['faq', 'часто задаваемые', 'вопросы и ответы',
                    'база знаний', 'knowledge base', 'инструкции']

    for link in links:
        link_text = link.get_text().lower()
        link_href = link.get('href', '')

        if any(keyword in link_text for keyword in faq_keywords):
            features['has_kb_or_faq'] = True
            features['kb_url'] = urljoin(url, link_href)
            break

    # 8. ПОИСК ДОКАЗАТЕЛЬСТВ 10+ ЧЕЛОВЕК (САМОЕ ВАЖНОЕ!)
    # Уровень A: прямое упоминание числа
    size_patterns = [
        r'(\d+)\s*сотрудник[а-я]*\s*поддержк',
        r'(\d+)\s*специалист[а-я]*\s*поддержк',
        r'(\d+)\s*оператор[а-я]*\s*поддержк',
        r'поддержка\s*из\s*(\d+)\s*человек',
        r'контакт-центр\s*(\d+)\s*оператор',
        r'(\d+)\s*человек\s*в\s*поддержке',
        r'команда\s*поддержки\s*в\s*(\d+)\s*человек'
    ]

    for pattern in size_patterns:
        match = re.search(pattern, text_lower, re.IGNORECASE)
        if match:
            try:
                team_size = int(match.group(1))
                if team_size >= 10:
                    # Нашли прямое доказательство (Уровень A)
                    features['support_team_size_min'] = team_size
                    features['support_evidence'] = f"Прямое упоминание на сайте: '{match.group(0)}'"
                    features['evidence_url'] = url
                    break
            except:
                continue

    return features


def analyze_company_website(company: Dict) -> Dict:
    """
    Анализ сайта одной компании
    """
    company_name = company.get('name', 'Unknown')
    site_url = company.get('site_url', '')

    print(f"\n🔍 Анализ: {company_name}")
    print(f"   URL: {site_url}")

    if not site_url or pd.isna(site_url):
        print("   ⚠️  Нет URL для анализа")
        return company

    # Нормализуем URL
    site_url = normalize_url(site_url)

    # Делаем запрос
    response = safe_request(site_url)

    if not response:
        print("   ❌ Не удалось загрузить сайт")
        return company

    # Ищем признаки поддержки
    features = find_support_features(response.text, site_url, company_name)

    # Объединяем данные компании с найденными признаками
    result = {**company, **features}

    # Выводим результаты
    found_features = []
    if features['has_support_email']: found_features.append("email")
    if features['has_contact_form']: found_features.append("форма")
    if features['has_online_chat']: found_features.append("чат")
    if features['has_support_section']: found_features.append("раздел")
    if features['has_kb_or_faq']: found_features.append("FAQ")
    if features['mentions_24_7']: found_features.append("24/7")

    if found_features:
        print(f"   ✅ Признаки: {', '.join(found_features)}")
    else:
        print(f"   ⚠️  Признаки не найдены")

    if features['support_team_size_min'] >= 10:
        print(f"   🎯 ДОКАЗАТЕЛЬСТВО 10+: {features['support_team_size_min']} человек")

    # Добавляем задержку чтобы не блокировать сайты
    time.sleep(1)

    return result


def save_enriched_data(enriched_df: pd.DataFrame, filename: str = 'data/raw/enriched_companies.csv'):
    """
    Сохраняем обогащенные данные
    """
    import os

    os.makedirs(os.path.dirname(filename), exist_ok=True)

    print(f"\n💾 Сохраняю результаты в: {filename}")
    print(f"📊 Размер данных: {len(enriched_df)} строк, {len(enriched_df.columns)} колонок")

    # Показываем первые 3 компании
    print("\n📋 Первые 3 записи:")
    for i, row in enriched_df.head(3).iterrows():
        print(f"  {i + 1}. {row['name']} - поддержка: {row.get('support_team_size_min', 0)} чел.")

    # Сохраняем
    enriched_df.to_csv(filename, index=False, encoding='utf-8-sig')

    # Проверяем что файл создан
    if os.path.exists(filename):
        file_size = os.path.getsize(filename)
        print(f"✅ Файл сохранен успешно! Размер: {file_size} байт")

        # Проверяем содержимое
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            print(f"📄 Всего строк в файле: {len(lines)}")

            if len(lines) > 1:
                print(f"📋 Заголовки: {lines[0].strip()}")
                print(f"📝 Первая запись: {lines[1].strip()[:100]}...")
            else:
                print("⚠️  В файле только заголовки!")
    else:
        print("❌ ОШИБКА: файл не создан!")

    return filename


def process_companies(companies_df: pd.DataFrame, limit: int = 20) -> pd.DataFrame:
    """
    Обработка списка компаний
    """
    print(f"\n{'=' * 60}")
    print("АНАЛИЗ САЙТОВ КОМПАНИЙ НА ПРИЗНАКИ ПОДДЕРЖКИ")
    print(f"{'=' * 60}")

    results = []

    # Ограничиваем для теста
    companies_to_process = companies_df.head(limit).copy()

    print(f"📊 Будут обработаны первые {len(companies_to_process)} компаний")
    print("ℹ️  Это может занять 2-3 минуты...")

    for idx, company in companies_to_process.iterrows():
        enriched_company = analyze_company_website(company.to_dict())
        results.append(enriched_company)

        # Показываем прогресс
        if (idx + 1) % 5 == 0:
            print(f"\n📈 Обработано: {idx + 1}/{len(companies_to_process)}")

    # Создаем DataFrame с результатами
    enriched_df = pd.DataFrame(results)

    # Статистика
    print(f"\n{'=' * 60}")
    print("📊 РЕЗУЛЬТАТЫ АНАЛИЗА:")
    print(f"{'=' * 60}")

    stats = {
        'Всего компаний': len(enriched_df),
        'С поддержкой 24/7': enriched_df['mentions_24_7'].sum() if 'mentions_24_7' in enriched_df.columns else 0,
        'С email поддержки': enriched_df[
            'has_support_email'].sum() if 'has_support_email' in enriched_df.columns else 0,
        'С контактной формой': enriched_df[
            'has_contact_form'].sum() if 'has_contact_form' in enriched_df.columns else 0,
        'С онлайн-чатом': enriched_df['has_online_chat'].sum() if 'has_online_chat' in enriched_df.columns else 0,
        'С разделом поддержки': enriched_df[
            'has_support_section'].sum() if 'has_support_section' in enriched_df.columns else 0,
        'С FAQ/Базой знаний': enriched_df['has_kb_or_faq'].sum() if 'has_kb_or_faq' in enriched_df.columns else 0,
        'С доказательством 10+': (enriched_df[
                                      'support_team_size_min'] >= 10).sum() if 'support_team_size_min' in enriched_df.columns else 0
    }

    for key, value in stats.items():
        print(f"   {key:25}: {value:3d}")

    # Сохраняем результаты
    save_enriched_data(enriched_df)

    return enriched_df


def main():
    """Основная функция парсинга сайтов"""
    try:
        # Загружаем компании
        companies_df = load_companies_from_csv()

        # Обрабатываем компании
        enriched_df = process_companies(companies_df, limit=20)

        print(f"\n✅ Анализ завершен!")
        print(f"✅ Обработано компаний: {len(enriched_df)}")

        # Рекомендации по дальнейшим шагам
        if 'support_team_size_min' in enriched_df.columns:
            companies_with_evidence = enriched_df[enriched_df['support_team_size_min'] >= 10]

            if len(companies_with_evidence) > 0:
                print(f"\n🎯 Компании с доказательствами 10+ человек:")
                for idx, company in companies_with_evidence.head(10).iterrows():
                    evidence = company.get('support_evidence', '')[:50]
                    print(f"   • {company['name']}: {company['support_team_size_min']} чел. - {evidence}...")
            else:
                print("\n⚠️  Не найдено компаний с прямыми доказательствами 10+")
                print("   Рекомендация: проверьте разделы 'Карьера' или 'Вакансии' на сайтах")

        return enriched_df

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()





