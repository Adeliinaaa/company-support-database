# Сбор первичного списка российских компаний
import pandas as pd
from typing import List, Dict
import time
import os

os.makedirs('data/raw', exist_ok=True)


def get_companies_from_manual_list() -> List[Dict]:
    """
    Ручной список известных компаний с крупной поддержкой
    """
    print("📋 Загрузка предопределенного списка компаний...")

    # Список компаний, где гарантированно есть крупная поддержка
    manual_companies = [
        # Банки (у всех большие контакт-центры)
        {'name': 'Сбербанк', 'site_url': 'https://www.sberbank.ru', 'industry': 'bank'},
        {'name': 'Тинькофф Банк', 'site_url': 'https://www.tinkoff.ru', 'industry': 'bank'},
        {'name': 'Альфа-Банк', 'site_url': 'https://alfabank.ru', 'industry': 'bank'},
        {'name': 'ВТБ', 'site_url': 'https://www.vtb.ru', 'industry': 'bank'},
        {'name': 'Газпромбанк', 'site_url': 'https://www.gazprombank.ru', 'industry': 'bank'},

        # Телеком (круглосуточная поддержка)
        {'name': 'МТС', 'site_url': 'https://mts.ru', 'industry': 'telecom'},
        {'name': 'Билайн', 'site_url': 'https://beeline.ru', 'industry': 'telecom'},
        {'name': 'МегаФон', 'site_url': 'https://megafon.ru', 'industry': 'telecom'},
        {'name': 'Tele2', 'site_url': 'https://tele2.ru', 'industry': 'telecom'},
        {'name': 'Ростелеком', 'site_url': 'https://rt.ru', 'industry': 'telecom'},

        # Маркетплейсы и ритейл (тысячи обращений в день)
        {'name': 'Wildberries', 'site_url': 'https://www.wildberries.ru', 'industry': 'retail'},
        {'name': 'OZON', 'site_url': 'https://www.ozon.ru', 'industry': 'retail'},
        {'name': 'Яндекс.Маркет', 'site_url': 'https://market.yandex.ru', 'industry': 'retail'},
        {'name': 'СИТИЛИНК', 'site_url': 'https://www.citilink.ru', 'industry': 'retail'},
        {'name': 'М.Видео', 'site_url': 'https://www.mvideo.ru', 'industry': 'retail'},
        {'name': 'Эльдорадо', 'site_url': 'https://www.eldorado.ru', 'industry': 'retail'},
        {'name': 'DNS', 'site_url': 'https://www.dns-shop.ru', 'industry': 'retail'},
        {'name': 'Лента', 'site_url': 'https://lenta.com', 'industry': 'retail'},
        {'name': 'Магнит', 'site_url': 'https://magnit.ru', 'industry': 'retail'},
        {'name': 'Пятерочка', 'site_url': 'https://5ka.ru', 'industry': 'retail'},

        # IT и интернет-компании
        {'name': 'Яндекс', 'site_url': 'https://yandex.ru', 'industry': 'it'},
        {'name': 'VK', 'site_url': 'https://vk.com', 'industry': 'it'},
        {'name': 'Рамблер', 'site_url': 'https://rambler.ru', 'industry': 'it'},
        {'name': '1С', 'site_url': 'https://1c.ru', 'industry': 'it'},
        {'name': 'Авито', 'site_url': 'https://www.avito.ru', 'industry': 'it'},
        {'name': 'Дром', 'site_url': 'https://www.drom.ru', 'industry': 'it'},
        {'name': 'Юла', 'site_url': 'https://youla.ru', 'industry': 'it'},
        {'name': '2ГИС', 'site_url': 'https://2gis.ru', 'industry': 'it'},

        # Страхование
        {'name': 'Ингосстрах', 'site_url': 'https://www.ingos.ru', 'industry': 'insurance'},
        {'name': 'Ренессанс Страхование', 'site_url': 'https://www.renins.com', 'industry': 'insurance'},
        {'name': 'СОГАЗ', 'site_url': 'https://www.sogaz.ru', 'industry': 'insurance'},
        {'name': 'АльфаСтрахование', 'site_url': 'https://alfastrah.ru', 'industry': 'insurance'},
        {'name': 'ВСК', 'site_url': 'https://www.vsk.ru', 'industry': 'insurance'},

        # Авиакомпании
        {'name': 'Аэрофлот', 'site_url': 'https://www.aeroflot.ru', 'industry': 'airline'},
        {'name': 'S7 Airlines', 'site_url': 'https://www.s7.ru', 'industry': 'airline'},
        {'name': 'Победа', 'site_url': 'https://www.pobeda.aero', 'industry': 'airline'},
        {'name': 'Уральские авиалинии', 'site_url': 'https://www.uralairlines.ru', 'industry': 'airline'},
        {'name': 'Россия', 'site_url': 'https://rossiya-airlines.com', 'industry': 'airline'},

        # Транспорт и логистика
        {'name': 'РЖД', 'site_url': 'https://www.rzd.ru', 'industry': 'transport'},
        {'name': 'Деловые Линии', 'site_url': 'https://www.dellin.ru', 'industry': 'logistics'},
        {'name': 'ПЭК', 'site_url': 'https://www.pecom.ru', 'industry': 'logistics'},
        {'name': 'СДЭК', 'site_url': 'https://www.cdek.ru', 'industry': 'logistics'},
        {'name': 'Boxberry', 'site_url': 'https://boxberry.ru', 'industry': 'logistics'},

        # Энергетика и промышленность
        {'name': 'Газпром', 'site_url': 'https://www.gazprom.ru', 'industry': 'energy'},
        {'name': 'Лукойл', 'site_url': 'https://lukoil.ru', 'industry': 'energy'},
        {'name': 'Роснефть', 'site_url': 'https://www.rosneft.ru', 'industry': 'energy'},
        {'name': 'Сургутнефтегаз', 'site_url': 'https://www.surgutneftegas.ru', 'industry': 'energy'},
        {'name': 'Татнефть', 'site_url': 'https://www.tatneft.ru', 'industry': 'energy'},

        # Онлайн-сервисы
        {'name': 'Яндекс.Такси', 'site_url': 'https://taxi.yandex.ru', 'industry': 'service'},
        {'name': 'Ситимобил', 'site_url': 'https://citimobil.ru', 'industry': 'service'},
        {'name': 'Delivery Club', 'site_url': 'https://www.deliveryclub.ru', 'industry': 'service'},
        {'name': 'Яндекс.Еда', 'site_url': 'https://eda.yandex.ru', 'industry': 'service'},
        {'name': 'СберМаркет', 'site_url': 'https://sbermarket.ru', 'industry': 'service'},

        # Игровые и развлекательные
        {'name': 'Wargaming', 'site_url': 'https://wargaming.net', 'industry': 'gaming'},
        {'name': 'Мирапринт', 'site_url': 'https://myprint.ru', 'industry': 'service'},
    ]

    print(f"✅ Загружено {len(manual_companies)} компаний из ручного списка")
    return manual_companies


def enrich_with_inn(companies: List[Dict]) -> List[Dict]:
    """
    Обогащаем данные ИНН (базовый маппинг)
    """
    print("🔎 Добавление ИНН для компаний...")

    # Маппинг ИНН для известных компаний
    inn_mapping = {
        'Сбербанк': '7707083893',
        'Тинькофф Банк': '7710140679',
        'Альфа-Банк': '7728168971',
        'ВТБ': '7736212660',
        'Газпромбанк': '7744001497',
        'МТС': '7740000076',
        'Билайн': '7713076301',
        'МегаФон': '7812014560',
        'Tele2': '5029223278',
        'Ростелеком': '7707049388',
        'Wildberries': '7728316484',
        'OZON': '1027739244741',
        'Яндекс': '7736207543',
        'VK': '7743001840',
        'Авито': '7724458880',
        'Ингосстрах': '7714017986',
        'Ренессанс Страхование': '7736019967',
        'СОГАЗ': '7707049388',
        'Аэрофлот': '7708511828',
        'S7 Airlines': '5408025106',
        'РЖД': '7708503727',
        'Деловые Линии': '3443011960',
        'Газпром': '7736050003',
        'Лукойл': '7706013788',
        'Яндекс.Такси': '7704340310',
        'Wargaming': '5902290393',
    }

    for company in companies:
        company_name = company['name']

        # Прямое совпадение
        if company_name in inn_mapping:
            company['inn'] = inn_mapping[company_name]
        else:
            # Попробуем найти частичное совпадение
            found = False
            for known_name, inn in inn_mapping.items():
                if known_name in company_name or company_name in known_name:
                    company['inn'] = inn
                    found = True
                    break

            if not found:
                company['inn'] = 'НЕ_НАЙДЕН'

    # Статистика
    inn_found = sum(1 for c in companies if c['inn'] != 'НЕ_НАЙДЕН')
    print(f"   Найдено ИНН для {inn_found} из {len(companies)} компаний")

    return companies


def save_companies_to_csv(companies: List[Dict], filename: str = None):
    """
    Сохраняем список компаний в CSV файл
    """
    import os

    if filename is None:
        # Явно указываем путь в папке проекта
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        filename = os.path.join(project_root, 'data', 'raw', 'companies_seed.csv')

    print(f"💾 Сохраняю в: {filename}")

    # Создаем папку если ее нет
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    df = pd.DataFrame(companies)

    # Сохраняем в CSV
    df.to_csv(filename, index=False, encoding='utf-8-sig')

    # Проверяем
    if os.path.exists(filename):
        print(f"✅ ФАЙЛ СОХРАНЕН УСПЕШНО!")
        print(f"📍 Местоположение: {filename}")
        print(f"📏 Размер: {os.path.getsize(filename)} байт")

        # Покажем первые 3 строки
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()[:4]
            print("\n📄 Содержимое файла (первые строки):")
            for line in lines:
                print(f"   {line.strip()}")
    else:
        print(f"❌ ОШИБКА: файл не создан!")

    print(f"📊 Всего компаний: {len(companies)}")

    # Статистика по отраслям
    if 'industry' in df.columns:
        industry_stats = df['industry'].value_counts()
        print("\n" + "📈 СТАТИСТИКА ПО ОТРАСЛЯМ:")
        print("   " + "-" * 30)

        for industry, count in industry_stats.items():
            print(f"   {industry:15} : {count:2d} компаний")

        print("   " + "-" * 30)
        print(f"   Всего отраслей: {len(industry_stats)}")

    # Выводим примеры компаний
    print("\n📋 ПРИМЕРЫ КОМПАНИЙ:")
    print("   " + "-" * 60)

    for i, company in enumerate(companies[:15], 1):
        inn = company.get('inn', 'НЕТ')
        name = company['name']
        industry = company.get('industry', '')

        # Обрезаем длинные названия
        if len(name) > 25:
            name_display = name[:22] + "..."
        else:
            name_display = name

        print(f"   {i:2d}. {name_display:25} | {industry:10} | ИНН: {inn}")

    return df


def main():
    """Основная функция сбора компаний"""
    print("=" * 60)
    print("СБОР ПЕРВИЧНОГО СПИСКА КОМПАНИЙ")
    print("=" * 60)

    # Даем небольшую задержку для наглядности
    time.sleep(1)

    # 1. Собираем компании из ручного списка
    companies = get_companies_from_manual_list()

    # 2. Обогащаем ИНН
    companies = enrich_with_inn(companies)

    # 3. Сохраняем
    df = save_companies_to_csv(companies)

    print("\n" + "=" * 60)
    print("✅ ПЕРВИЧНЫЙ СБОР ЗАВЕРШЕН!")
    print(f"✅ Всего собрано: {len(companies)} компаний")
    print("=" * 60)

    return df


if __name__ == "__main__":
    main()





