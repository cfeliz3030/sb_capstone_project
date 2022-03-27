def remove_columns(df):
    df = df.drop('tsun', 'coco')
    return df