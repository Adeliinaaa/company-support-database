# Парсинг сайтов компаний для поиска признаков поддержки и доказательств 10+ человек


import pandas as pd
import time
import re
import os
import random
from typing import Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass
import logging

# Установите эти библиотеки через pip, если их нет:
# pip install selenium beautifulsoup4 pandas fake-useragent webdriver-manager lxml
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent
import requests
from bs4 import BeautifulSoup

# Настройка логирования для отслеживания процесса
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ParsingResult:
    """Контейнер для результатов парсинга одной компании"""
    success: bool = False
    html: Optional[str] = None
    final_url: str = ""
    method: str = ""  # 'requests' или 'selenium'
    error: Optional[str] = None


# Класс для умной загрузки страниц (гибридный подход)
class SmartPageLoader:
    """Умный загрузчик страниц: сначала пробует requests, если не выходит - Selenium"""

    def __init__(self):
        self.ua = UserAgent()
        self.session = requests.Session()
        self.driver = None
        self.setup_requests_session()

    def setup_requests_session(self):
        """Настройка сессии requests с рандомными User-Agent"""
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        })

    def get_with_requests(self, url: str, timeout: int = 15) -> ParsingResult:
        """Попытка загрузки через requests"""
        try:
            # Меняем User-Agent для каждого запроса
            self.session.headers['User-Agent'] = self.ua.random

            response = self.session.get(
                url,
                timeout=timeout,
                allow_redirects=True,
                verify=False  # Внимание: отключает проверку SSL, для продакшена уберите
            )

            # Проверяем, не получили ли мы капчу или блокировку
            if response.status_code == 403 or "captcha" in response.text.lower():
                return ParsingResult(success=False, error=f"Блокировка (статус {response.status_code})")

            if response.status_code == 200:
                return ParsingResult(
                    success=True,
                    html=response.text,
                    final_url=response.url,
                    method='requests'
                )
            else:
                return ParsingResult(success=False, error=f"HTTP {response.status_code}")

        except Exception as e:
            return ParsingResult(success=False, error=f"Requests error: {str(e)}")

    def init_selenium_driver(self):
        """Инициализация Selenium драйвера в headless-режиме (без графического интерфейса)[citation:3]"""
        if self.driver is None:
            chrome_options = Options()

            # Headless режим для работы без открытия браузера
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")

            # Рандомный User-Agent[citation:3]
            chrome_options.add_argument(f"user-agent={self.ua.random}")

            # Другие параметры для маскировки под обычный браузер
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)

            try:
                # Автоматическая установка ChromeDriver
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)

                # Скрываем WebDriver признаки
                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

                logger.info("Selenium драйвер инициализирован")
            except Exception as e:
                logger.error(f"Ошибка инициализации Selenium: {e}")
                raise

    def get_with_selenium(self, url: str, timeout: int = 30) -> ParsingResult:
        """Загрузка через Selenium (для JavaScript-сайтов)[citation:3][citation:9]"""
        try:
            if self.driver is None:
                self.init_selenium_driver()

            self.driver.get(url)

            # Ждем загрузки страницы
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )

            # Небольшая дополнительная пауза для динамического контента
            time.sleep(random.uniform(2, 4))

            # Прокрутка для загрузки ленивого контента
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(1)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)

            return ParsingResult(
                success=True,
                html=self.driver.page_source,
                final_url=self.driver.current_url,
                method='selenium'
            )

        except TimeoutException:
            return ParsingResult(success=False, error="Selenium timeout")
        except Exception as e:
            return ParsingResult(success=False, error=f"Selenium error: {str(e)}")

    def smart_get(self, url: str) -> ParsingResult:
        """
        Умная загрузка: сначала requests, если не вышло - Selenium
        Возвращает: (успех, html, использованный_метод, ошибка)
        """
        logger.info(f"Пытаемся загрузить: {url}")

        # Сначала пробуем быстрый способ
        result = self.get_with_requests(url)

        # Если requests не сработал, пробуем Selenium
        if not result.success:
            logger.info(f"Requests не удался ({result.error}), пробуем Selenium...")
            result = self.get_with_selenium(url)

        # Реалистичная пауза между запросами (от 3 до 7 секунд)[citation:8]
        time.sleep(random.uniform(3, 7))

        return result

    def close(self):
        """Закрытие драйвера"""
        if self.driver:
            self.driver.quit()
            self.driver = None



# Анализатор контента
class EnhancedContentAnalyzer:
    """Улучшенный анализатор контента с лучшими шаблонами поиска"""

    def __init__(self):
        # Улучшенные регулярные выражения для email[citation:1]
        self.email_patterns = [
            # Стандартные email
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            # Email в mailto ссылках[citation:1]
            r'mailto:([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})',
            # Специфичные для поддержки
            r'\b(support|help|info|service|contact|поддержка|помощь)@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        ]

        # Ключевые слова для поиска (русские и английские)
        self.support_keywords = {
            'support_section': [
                'поддержк', 'помощь', 'help', 'support', 'служба поддержки',
                'контакт-центр', 'техподдержка', 'customer support', 'service center',
                'контакты', 'contacts', 'обратная связь', 'contact us',
                'клиентская служба', 'customer service'
            ],
            'faq_kb': [
                'faq', 'часто задаваемые', 'вопросы и ответы', 'база знаний',
                'knowledge base', 'инструкции', 'help center', 'справка',
                'вопрос-ответ', 'q&a', 'руководство'
            ],
            'contact_form': [
                'форма обратной связи', 'контактная форма', 'напишите нам',
                'задать вопрос', 'contact form', 'feedback form', 'связаться с нами',
                'отправить сообщение'
            ],
            '24_7': [
                '24/7', '24 часа', 'круглосуточно', 'круглые сутки',
                'работаем без выходных', 'всегда на связи', '24 часа в сутки',
                'non-stop', 'always available'
            ]
        }

        # Шаблоны для размера команды поддержки
        self.team_size_patterns = [
            # Прямые упоминания с числами
            r'(\d+)\s*(?:сотрудник|специалист|оператор|человек|менеджер)[а-я]+\s+поддержк',
            r'поддержк[а-я]+\s+(?:из|в)\s+(\d+)\s+(?:сотрудник|специалист|человек)',
            r'контакт-центр\s+(?:из|на|в)\s+(\d+)\s+(?:оператор|сотрудник|человек)',
            r'(\d+)\s+(?:оператор|сотрудник)\s+(?:в\s+)?контакт-центр',
            r'команд[а-я]+\s+поддержки\s+(?:в\s+)?(\d+)\s+(?:человек|сотрудник)',
            # Более общие, но полезные
            r'более\s+(\d+)\s+(?:сотрудник|специалист|человек)\s+(?:работает|в)',
            r'штат\s+(?:из|в)\s+(\d+)\s+(?:сотрудник|человек)',
            r'(\d+)\+?\s+(?:сотрудник|специалист)\s+(?:в отделе|в службе)'
        ]

        # Признаки онлайн-чата (ищем в скриптах и коде)
        self.chat_indicators = [
            'jivo', 'livechat', 'chatra', 'drift', 'tawk.to', 'zopim',
            'intercom', 'crisp', 'olark', 'purechat', 'userlike',
            'livechatinc', 'tidio', 'helpcrunch', 'chat-widget',
            'online-chat', 'чат-виджет', 'виджет чата'
        ]

        # Мессенджеры
        self.messenger_patterns = [
            r't\.me/[\w]+', r'telegram\.me/[\w]+',
            r'wa\.me/[\d]+', r'whatsapp\.com/send\?phone=[\d]+',
            r'viber\.me/[\w]+', r'vk\.me/[\w]+', r'vk\.com/im\?sel=[\d]+',
            r'messenger\.com/t/[\w\.]+', r'facebook\.com/messages/t/[\w\.]+'
        ]

    def analyze(self, html: str, url: str) -> Dict:
        """Полный анализ HTML на признаки поддержки"""

        soup = BeautifulSoup(html, 'lxml')
        text = soup.get_text().lower()

        # Базовый результат
        result = {
            # Основные поля по ТЗ
            'has_support_email': False,
            'has_contact_form': False,
            'has_online_chat': False,
            'has_messengers': False,
            'has_support_section': False,
            'has_kb_or_faq': False,
            'mentions_24_7': False,
            'support_team_size_min': 0,
            'support_evidence': '',
            'evidence_url': url,
            'evidence_type': 'site',
            'support_email': '',
            'support_url': '',
            'kb_url': '',
            'chat_vendor': '',
            'source': 'company_site_improved',

            # Дополнительная информация для отладки
            'page_title': soup.title.string if soup.title else '',
            'analysis_method': 'combined'
        }

        # 1. Поиск email поддержки
        result.update(self._find_support_emails(text, soup))

        # 2. Поиск контактной формы
        result['has_contact_form'] = self._find_contact_form(text, soup)

        # 3. Поиск онлайн-чата
        chat_info = self._find_online_chat(text, str(soup))
        result['has_online_chat'] = chat_info['found']
        result['chat_vendor'] = chat_info['vendor']

        # 4. Поиск мессенджеров
        result['has_messengers'] = self._find_messengers(text)

        # 5. Поиск раздела поддержки и FAQ
        support_info = self._find_support_sections(soup, url, text)
        result['has_support_section'] = support_info['has_support']
        result['support_url'] = support_info['support_url']
        result['has_kb_or_faq'] = support_info['has_faq']
        result['kb_url'] = support_info['kb_url']

        # 6. Поиск 24/7
        result['mentions_24_7'] = self._find_24_7(text)

        # 7. Поиск доказательств 10+ человек (САМОЕ ВАЖНОЕ!)
        team_evidence = self._find_team_size_evidence(text, url)
        if team_evidence['size'] >= 10:
            result['support_team_size_min'] = team_evidence['size']
            result['support_evidence'] = team_evidence['evidence']
        elif result['mentions_24_7']:
            # Если есть 24/7, но нет точного числа - ставим 10
            result['support_team_size_min'] = 10
            result['support_evidence'] = "Поддержка 24/7 (сменный график требует минимум 10 человек)"

        return result

    def _find_support_emails(self, text: str, soup: BeautifulSoup) -> Dict:
        """Поиск email адресов поддержки"""
        result = {'has_support_email': False, 'support_email': ''}

        # Ищем во всем тексте
        for pattern in self.email_patterns:
            emails = re.findall(pattern, text, re.IGNORECASE)
            for email in emails:
                # Проверяем, что это email поддержки, а не общий
                email_lower = email.lower()
                if any(keyword in email_lower for keyword in ['support', 'help', 'info', 'contact',
                                                              'поддерж', 'помощь', 'контакт']):
                    result['has_support_email'] = True
                    result['support_email'] = email
                    return result

        # Ищем в mailto ссылках
        mailto_links = soup.find_all('a', href=lambda x: x and x.startswith('mailto:'))
        for link in mailto_links:
            email = link['href'].replace('mailto:', '')
            if email:
                result['has_support_email'] = True
                result['support_email'] = email
                break

        return result

    def _find_contact_form(self, text: str, soup: BeautifulSoup) -> bool:
        """Поиск контактной формы"""
        # По ключевым словам в тексте
        for keyword in self.support_keywords['contact_form']:
            if keyword in text:
                return True

        # По наличию форм с определенными атрибутами
        forms = soup.find_all('form')
        for form in forms:
            form_html = str(form).lower()
            form_action = form.get('action', '').lower()
            form_id = form.get('id', '').lower()
            form_class = form.get('class', [])
            form_class = ' '.join(form_class).lower() if form_class else ''

            # Проверяем различные признаки контактной формы
            contact_indicators = ['contact', 'feedback', 'form', 'сообщен', 'письм']
            if any(indicator in form_action for indicator in contact_indicators) or \
                    any(indicator in form_id for indicator in contact_indicators) or \
                    any(indicator in form_class for indicator in contact_indicators):
                return True

        return False

    def _find_online_chat(self, text: str, html: str) -> Dict:
        """Поиск онлайн-чата"""
        result = {'found': False, 'vendor': ''}

        html_lower = html.lower()

        # Ищем признаки чата в HTML (часто в скриптах)
        for vendor in self.chat_indicators:
            if vendor in html_lower:
                result['found'] = True
                result['vendor'] = vendor
                break

        # Дополнительные проверки
        if not result['found']:
            chat_keywords = ['чат', 'online chat', 'live chat', 'онлайн-чат', 'chat widget']
            if any(keyword in text for keyword in chat_keywords):
                result['found'] = True
                result['vendor'] = 'unknown'

        return result

    def _find_messengers(self, text: str) -> bool:
        """Поиск ссылок на мессенджеры"""
        for pattern in self.messenger_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        return False

    def _find_support_sections(self, soup: BeautifulSoup, base_url: str, text: str) -> Dict:
        """Поиск разделов поддержки и FAQ"""
        result = {
            'has_support': False,
            'support_url': '',
            'has_faq': False,
            'kb_url': ''
        }

        # Ищем ссылки на поддержку
        links = soup.find_all('a', href=True)

        for link in links:
            link_text = link.get_text().lower()
            link_href = link.get('href', '').lower()

            # Проверяем раздел поддержки
            if not result['has_support']:
                for keyword in self.support_keywords['support_section']:
                    if keyword in link_text or keyword in link_href:
                        result['has_support'] = True
                        result['support_url'] = urljoin(base_url, link['href'])
                        break

            # Проверяем FAQ/базу знаний
            if not result['has_faq']:
                for keyword in self.support_keywords['faq_kb']:
                    if keyword in link_text or keyword in link_href:
                        result['has_faq'] = True
                        result['kb_url'] = urljoin(base_url, link['href'])
                        break

            # Если нашли оба, прерываем поиск
            if result['has_support'] and result['has_faq']:
                break

        return result

    def _find_24_7(self, text: str) -> bool:
        """Поиск упоминаний 24/7 поддержки"""
        for keyword in self.support_keywords['24_7']:
            if keyword in text:
                return True
        return False

    def _find_team_size_evidence(self, text: str, url: str) -> Dict:
        """Поиск доказательств размера команды поддержки"""
        result = {'size': 0, 'evidence': ''}

        for pattern in self.team_size_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                try:
                    # match может быть строкой или кортежем
                    if isinstance(match, tuple):
                        match = match[0]

                    team_size = int(match)
                    if team_size >= 10:
                        # Находим контекст для доказательства
                        start = max(0, text.find(match) - 100)
                        end = min(len(text), text.find(match) + 100)
                        context = text[start:end].replace('\n', ' ').strip()

                        result['size'] = team_size
                        result['evidence'] = f"На сайте указано: '{context[:150]}...'"
                        return result
                except (ValueError, TypeError):
                    continue

        return result

# Основной рабочий процесс
def analyze_single_company(company: Dict, loader: SmartPageLoader,
                           analyzer: EnhancedContentAnalyzer) -> Dict:
    """Анализ одной компании"""
    company_name = company.get('name', 'Unknown')
    site_url = company.get('site_url', '')

    print(f"\n🔍 Анализ: {company_name}")
    print(f"   URL: {site_url}")

    # Базовый результат с данными компании
    result = {
        'name': company_name,
        'site': site_url,
        'inn': company.get('inn', ''),
        'industry': company.get('industry', ''),
        'parsing_success': False,
        'parsing_method': '',
        'parsing_error': ''
    }

    if not site_url or pd.isna(site_url):
        print("   ⚠️  Нет URL для анализа")
        result['parsing_error'] = 'No URL'
        return result

    try:
        # Загружаем страницу
        page_result = loader.smart_get(site_url)

        if not page_result.success:
            print(f"   ❌ Не удалось загрузить сайт: {page_result.error}")
            result['parsing_error'] = page_result.error
            return result

        result['parsing_success'] = True
        result['parsing_method'] = page_result.method
        result['final_url'] = page_result.final_url

        # Анализируем контент
        analysis_result = analyzer.analyze(page_result.html, page_result.final_url)

        # Объединяем результаты
        result.update(analysis_result)

        # Выводим краткие результаты
        features = []
        if result['has_support_email']: features.append("📧 email")
        if result['has_contact_form']: features.append("📝 форма")
        if result['has_online_chat']: features.append("💬 чат")
        if result['has_messengers']: features.append("📱 мессенджеры")
        if result['has_support_section']: features.append("🆘 раздел")
        if result['has_kb_or_faq']: features.append("❓ FAQ")
        if result['mentions_24_7']: features.append("⏰ 24/7")

        if features:
            print(f"   ✅ Признаки: {', '.join(features)}")

        if result['support_team_size_min'] >= 10:
            print(f"   🎯 ДОКАЗАТЕЛЬСТВО: {result['support_team_size_min']}+ человек")
            print(f"   📋 Доказательство: {result['support_evidence'][:80]}...")
        elif result['mentions_24_7']:
            print(f"   ⏰ Поддержка 24/7 (оценка: 10+ человек)")

        print(f"   🛠️ Метод: {page_result.method}")

    except Exception as e:
        print(f"   ❌ Ошибка при анализе: {e}")
        result['parsing_error'] = str(e)

    return result


def main():
    """Основная функция"""
    print("=" * 70)
    print("🚀 УЛУЧШЕННЫЙ ПАРСИНГ САЙТОВ КОМПАНИЙ")
    print("=" * 70)

    loader = None
    try:
        # Загружаем компании
        input_path = 'data/raw/companies_seed.csv'
        if not os.path.exists(input_path):
            # Пробуем альтернативные пути
            possible_paths = [
                'companies_seed.csv',
                '../data/raw/companies_seed.csv',
                os.path.join(os.path.dirname(__file__), '../data/raw/companies_seed.csv')
            ]

            for path in possible_paths:
                if os.path.exists(path):
                    input_path = path
                    break

        if not os.path.exists(input_path):
            print(f"❌ Файл с компаниями не найден")
            print(f"   Сначала запустите collect_seeds.py")
            return

        df = pd.read_csv(input_path, encoding='utf-8-sig')
        print(f"📁 Загружено {len(df)} компаний")

        # Инициализируем загрузчик и анализатор
        loader = SmartPageLoader()
        analyzer = EnhancedContentAnalyzer()

        # Ограничиваем количество для теста (можно увеличить)
        limit = 30
        companies_to_process = df.head(limit).copy()

        print(f"\n📊 Будут обработаны первые {len(companies_to_process)} компаний")
        print("⏳ Это займет 10-15 минут из-за пауз между запросами...")

        results = []
        for idx, company in companies_to_process.iterrows():
            print(f"\n[{idx + 1}/{len(companies_to_process)}] ", end="")
            result = analyze_single_company(company.to_dict(), loader, analyzer)
            results.append(result)

        # Создаем DataFrame с результатами
        result_df = pd.DataFrame(results)

        # Статистика
        print(f"\n{'=' * 70}")
        print("📊 ДЕТАЛЬНАЯ СТАТИСТИКА:")
        print(f"{'=' * 70}")

        if len(result_df) > 0:
            stats = [
                ('Всего компаний', len(result_df)),
                ('Успешно загружено', result_df['parsing_success'].sum()),
                ('Через Requests', (result_df['parsing_method'] == 'requests').sum()),
                ('Через Selenium', (result_df['parsing_method'] == 'selenium').sum()),
                ('', ''),
                ('С email поддержки', result_df['has_support_email'].sum()),
                ('С контактной формой', result_df['has_contact_form'].sum()),
                ('С онлайн-чатом', result_df['has_online_chat'].sum()),
                ('С мессенджерами', result_df['has_messengers'].sum()),
                ('С разделом поддержки', result_df['has_support_section'].sum()),
                ('С FAQ/Базой знаний', result_df['has_kb_or_faq'].sum()),
                ('С поддержкой 24/7', result_df['mentions_24_7'].sum()),
                ('', ''),
                ('С доказательствами 10+', (result_df['support_team_size_min'] >= 10).sum())
            ]

            for label, value in stats:
                if label == '':
                    print("   " + "-" * 40)
                else:
                    print(f"   {label:30}: {value:3d}")

        # Сохраняем результаты
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_dir = 'data/raw'
        os.makedirs(output_dir, exist_ok=True)

        output_path = f'{output_dir}/enriched_companies_improved_{timestamp}.csv'
        result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\n💾 Основные результаты сохранены в: {output_path}")

        # Также сохраняем постоянный файл
        permanent_path = f'{output_dir}/enriched_companies_improved.csv'
        result_df.to_csv(permanent_path, index=False, encoding='utf-8-sig')
        print(f"💾 Постоянная копия: {permanent_path}")

        # Показываем компании с доказательствами
        if 'support_team_size_min' in result_df.columns:
            companies_with_evidence = result_df[result_df['support_team_size_min'] >= 10]

            if len(companies_with_evidence) > 0:
                print(f"\n{'=' * 70}")
                print("🎯 КОМПАНИИ С ДОКАЗАТЕЛЬСТВАМИ 10+ ЧЕЛОВЕК:")
                print(f"{'=' * 70}")

                for idx, (_, row) in enumerate(companies_with_evidence.iterrows(), 1):
                    evidence_short = row['support_evidence']
                    if len(evidence_short) > 80:
                        evidence_short = evidence_short[:77] + "..."

                    print(f"   {idx:2d}. {row['name'][:35]:35} | {int(row['support_team_size_min']):3d}+ чел.")
                    print(f"       📋 {evidence_short}")
                    if row.get('support_url'):
                        print(f"       🔗 {row['support_url'][:60]}...")
                    print()

        print(f"\n✅ Анализ завершен!")
        print(f"✅ Обработано: {len(result_df)} компаний")
        print(f"✅ Успешно: {result_df['parsing_success'].sum()} компаний")

        return result_df

    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if loader:
            loader.close()


if __name__ == "__main__":
    main()










