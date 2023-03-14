def kr_en_convert(content):
    tables_dict = {
        '시:분': 'DATETIME',
        '강수': 'RAIN',
        '강수15': 'RAIN_15M',
        '강수60': 'RAIN_60M',
        '강수3H': 'RAIN_3H',
        '강수6H': 'RAIN_6H',
        '강수12H': 'RAIN_12H',
        '일강수': 'RAIN_1D',
        '기온': 'TEMP',
        '풍향1': 'WIND_DIRECTION_1M',
        '풍향1_D': 'WIND_DIRECTION_1M_STRING',
        '풍속1(m/s)': 'WIND_SPEED_1M',
        '풍향10': 'WIND_DIRECTION_10M',
        '풍향10_D': 'WIND_DIRECTION_10M_STRING',
        '풍속10(m/s)': 'WIND_SPEED_10M',
        '습도': 'HUM',
        '해면기압': 'HPA'
    }
    col_list = []
    for col in content:
        if col.attrs.get('colspan') == str(2):
            col_list.append(tables_dict.get(col.text))
            col_list.append(tables_dict.get(col.text + '_D'))
        else:
            col_list.append(tables_dict.get(col.text))
    return col_list


def columns_type(col):
    tables_types = {
        'DATETIME': str, 'RAIN': str, 'RAIN_15M': float, 'RAIN_60M': float, 'RAIN_3H': float, 'RAIN_6H': float,
        'RAIN_12H': float, 'RAI_N1D': float, 'TEMP': float, 'WIND_DIRECTION_1M': float, 'WIND_DIRECTION_1M_STRING': str,
        'WIND_SPEED_1M': float, 'WIND_DIRECTION_10M': float, 'WIND_DIRECTION_10Ms': str, 'WIND_SPEED_10M': float,
        'HUM': float, 'HPA': float}

    return tables_types.get(col)
