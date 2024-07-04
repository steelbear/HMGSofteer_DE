'''
etl_project_gdp: 나라별 GDP 리스트를 위키피디아에서 가져오기
'''

import logging
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup, Tag


# 나라별 GDP 위키피디아 URL
GDP_WIKI_URL = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

# 대륙 이름과 위키피디아 내 대륙별 소속 나라 리스트가 포함된 챕터 id
CONTINENT_COUNTRY_TABLE_URLS = [
    ['Asia', 'States_of_Asia'],
    ['North_America', 'Definitions'],
    ['South_America', 'Countries_and_territories'],
    ['Europe', 'List_of_states_and_territories'],
    ['Africa', 'Territories_and_regions'],
    ['Oceania', 'Demographics'],
]


def extract_gdp_table() -> pd.DataFrame:
    '''
    위키피디아에서 IMF가 조사한 GDP 테이블을 pd.DataFrame 형으로 가져오기
    '''
    normalization_dict = {
        'DR Congo': 'Democratic Republic of the Congo',
        'Congo': 'Republic of the Congo',
        'Bahamas': 'The Bahamas',
        'Gambia': 'The Gambia',
    }

    try:
        req_wiki_gdp = requests.get(GDP_WIKI_URL, timeout=10)
    except requests.exceptions.Timeout as e:
        raise e

    soup = BeautifulSoup(req_wiki_gdp.text, 'html.parser')

    gdp_list = []
    gdp_table = soup.find(id='Table').parent \
                    .find_next_sibling('table').find_all('tr') # 'Table' 챕터에 들어있는 테이블 찾기

    for row in gdp_table[3:]: # <thead>에 있는 <tr>과 World 행을 제외
        row = row.get_text().split('\n')
        country, gdp = row[1:3]
        country = country[1:].strip()

        if country in normalization_dict:
            country = normalization_dict.get(country)

        if gdp == '—': # 공란 처리된 셀을 Null로 처리
            gdp = None
        else:
            gdp = float(gdp.replace(',', ''))
            gdp = gdp / 1000  # million$ 단위에서 billion$ 단위로 변환
            gdp = round(gdp, 2)
        gdp_list.append({'country': country, 'gdp': gdp})

    return pd.DataFrame(gdp_list)


def extract_from_html_table(table_tag: Tag, continent: str) -> list[dict[str, str]]:
    '''
    파싱된 테이블에서 나라 리스트를 가져와 (나라, 대륙) 쌍 데이터를 가져오기
    '''
    output_list = []

    tr_elements = table_tag.find_all('tr')

    for tr in tr_elements:
        row = tr.text.split('\n')
        if row[1].strip() != '': # 테이블 내 그룹 표기(예: 동유럽, 서유럽, 북유럽)를 나타내는 행 제거
            continue
        country = re.sub(r'\[.+\]|\ ?\(.+\)|\*', r'', row[5]) # 첨자와 속령 표기 제거
        output_list.append({'country': country, 'continent': continent})

    return output_list


def extract_continent_country_table() -> pd.DataFrame:
    '''
    위키피디아에서 대륙별 나라 리스트를 가져와 pd.DataFrame 형에 모두 모아 가져오기
    '''
    country_continent_pairs = []

    for continent, section_id in CONTINENT_COUNTRY_TABLE_URLS:
        wiki_url = 'https://en.wikipedia.org/wiki/' + continent # 대륙별 위키 페이지 주소
        try:
            req = requests.get(wiki_url, timeout=10)
        except requests.exceptions.Timeout as e:
            raise e
        
        soup = BeautifulSoup(req.text, 'html.parser')

        # 나라 리스트가 포함된 챕터에서 테이블 가져오기
        table = soup.find(id=section_id).parent.find_next_sibling('table')
        # 유럽의 경우, 나라 리스트는 두번째 테이블에 존재
        if continent == 'Europe':
            table = table.next_sibling.next_sibling
        
        pairs = extract_from_html_table(table, continent)
        country_continent_pairs.extend(pairs)

        # 아시아와 유럽은 UN이 인정하지 않은 국가 리스트가 별도로 존재
        if continent in ['Asia', 'Europe']:
            table = table.next_sibling.next_sibling.next_sibling.next_sibling
            pairs = extract_from_html_table(table, continent)
            country_continent_pairs.extend(pairs)
        
    return pd.DataFrame(country_continent_pairs)


def get_top5_mean(df: pd.DataFrame) -> float:
    '''
    GDP 상위 5개 나라의 GDP 평균 계산하기
    '''
    top5_gdp_s = df['gdp'].sort_values(ascending=False).head(5)
    return top5_gdp_s.mean()


def show_table(title: str, df: pd.DataFrame) -> None:
    '''
    pd.DataFrame을 CLI 화면에 표시
    '''
    print(title)
    print(df.index.name, end='\t')
    for column in df.columns:
        print(column, end='\t')
    print()
    
    for index, row in df.iterrows():
        print(index, end='\t')
        print('\t'.join(map(str, row.values)))
    print()


def main() -> None:
    logging.basicConfig(
        filename='elt_project_log.txt',
        format='%(asctime)s, %(message)s',
        datefmt='%Y-%b-%d-%H-%M-%S',
        level=logging.INFO)

    # ----- Data Extraction ----- #
    logging.info('Extracting GDP table from wikipedia ...')
    try:
        gdp_df = extract_gdp_table()
    except requests.exceptions.Timeout as e:
        logging.error(e)
        return
    else:
        logging.info('Successfully extracted GDP table')

    logging.info('Extracting Country-Continent table from wikipedia ...')
    try:
        country_continent_df = extract_continent_country_table()
    except requests.exceptions.Timeout as e:
        logging.error(e)
        return
    else:
        logging.info('Successfully extracted Country-Continent table')
    # ----- Data Extraction ----- #

    # ----- Data Transformation ----- #
    logging.info('Transforming GDP table ...')
    gdp_df.dropna(subset=['gdp'], inplace=True)
    gdp_df.sort_values('gdp', ascending=False, inplace=True)
    gdp_df.set_index('country', inplace=True)
    show_table('GDP by countries more than $100B', gdp_df[gdp_df['gdp'] >= 100]) # $100B 이상만 출력
    logging.info('Successfully transformed GDP table')

    logging.info('Transforming for Top 5 mean GDP by continents ...')
    country_continent_df.set_index('country', inplace=True)
    top5_mean_gdp_by_continent_df = gdp_df.join(country_continent_df, on='country') \
                                          .groupby('continent') \
                                          .apply(lambda x: round(get_top5_mean(x), 2), 
                                                 include_groups=False)
    show_table('Top 5 mean GDP by continents', 
               pd.DataFrame({'Top 5 mean GDP': top5_mean_gdp_by_continent_df}))
    logging.info('Successfully transformed Top 5 mean GDP table')
    # ----- Data Transformation ----- #

    # ----- Data Load ----- #
    logging.info('Loading GDP table to disk ...')
    try:
        gdp_df.to_json('Countries_by_GDP.json')
    except Exception as e:
        logging.error(e)
    else:
        logging.info('Successfully loaded GDP time')
    # ----- Data Load ----- #


if __name__ == '__main__':
    main()
