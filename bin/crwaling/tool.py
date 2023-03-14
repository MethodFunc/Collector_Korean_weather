def kr_en_convert(content):
    tables_dict = {
        '시:분': 'DATETIME',
        '강수': 'rain',
        '강수15': 'rain15',
        '강수60': 'rain60',
        '강수3H': 'rain3h',
        '강수6H': 'rain6h',
        '강수12H': 'rain12h',
        '일강수': 'rain1d',
        '기온': 'temp',
        '풍향1': 'wd1',
        '풍향1_D': 'wd1s',
        '풍속1(m/s)': 'ws1',
        '풍향10': 'wd10',
        '풍향10_D': 'wd10s',
        '풍속10(m/s)': 'ws10',
        '습도': 'hum',
        '해면기압': 'hpa'
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
        'DATETIME': str, 'rain': str, 'rain15': float, 'rain60': float, 'rain3h': float, 'rain6h': float,
        'rain12h': float, 'rain1d': float, 'temp': float, 'wd1': float, 'wd1s': str, 'ws1': float, 'wd10': float,
        'wd10s': str, 'ws10': float, 'hum': float, 'hpa': float}

    return tables_types.get(col)
