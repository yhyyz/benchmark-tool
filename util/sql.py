import re

def remove_sql_comments(sql):
    # 删除以 -- 开头的整行注释
    sql = re.sub(r'^\s*--.*$', '', sql, flags=re.MULTILINE)
    # 删除行尾的 -- 注释
    sql = re.sub(r'--.*$', '', sql)
    # 删除多余的空行
    sql = re.sub(r'\n\s*\n', '\n', sql)
    return sql.strip()