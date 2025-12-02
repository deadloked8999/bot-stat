# Тест логики объединения СБ
from collections import defaultdict

# Имитируем данные
ops_m = [
    {'code': 'СБ', 'name': 'Роман Смолин', 'channel': 'нал', 'amount': 72400},
    {'code': 'СБ', 'name': 'Смолин Роман', 'channel': 'нал', 'amount': 8000},
    {'code': 'СБ', 'name': 'Роман Смолин', 'channel': 'безнал', 'amount': 40600},
    {'code': 'СБ', 'name': 'Смолин Роман', 'channel': 'безнал', 'amount': 4700},
    {'code': 'СБ', 'name': 'Александр Ромашкан', 'channel': 'нал', 'amount': 1000},
    {'code': 'СБ', 'name': 'Денис Ермаков', 'channel': 'нал', 'amount': 1000},
]

ops_a = [
    {'code': 'СБ', 'name': 'Денис Ермаков', 'channel': 'нал', 'amount': 30100},
    {'code': 'СБ', 'name': 'Дмитрий Васенев', 'channel': 'нал', 'amount': 8000},
]

# Словари объединений
sb_merges_moskvich = {'Смолин Роман': 'Роман Смолин'}
sb_merges_anora = {}

# sb_cross_club_matches (СБ между клубами)
sb_cross_club_matches = [
    {'name_moskvich': 'Денис Ермаков', 'name_anora': 'Денис Ермаков',
     'moskvich': {'nal': 1000, 'beznal': 8850}, 'anora': {'nal': 30100, 'beznal': 23600}}
]

processed = set()
merged_ops = []

# 1. Обработка sb_cross_club_matches (объединяем)
for match in sb_cross_club_matches:
    name_m = match['name_moskvich']
    name_a = match['name_anora']
    united_name = max(name_m, name_a, key=len)
    total_nal = match['moskvich']['nal'] + match['anora']['nal']
    total_beznal = match['moskvich']['beznal'] + match['anora']['beznal']
    
    if total_nal > 0:
        merged_ops.append({'code': 'СБ', 'name': united_name, 'channel': 'нал', 'amount': total_nal})
    if total_beznal > 0:
        merged_ops.append({'code': 'СБ', 'name': united_name, 'channel': 'безнал', 'amount': total_beznal})
    
    processed.add(('СБ', name_m))
    processed.add(('СБ', name_a))

print(f"После sb_cross_club_matches: processed={processed}")
print(f"merged_ops: {len(merged_ops)} операций")

# 2. Группируем СБ из Москвича
sb_moskvich_grouped = defaultdict(lambda: {'nal': 0, 'beznal': 0})
for op in ops_m:
    if op['code'] == 'СБ':
        name = op['name']
        # Применяем объединение
        if sb_merges_moskvich and name in sb_merges_moskvich:
            name = sb_merges_moskvich[name]
        
        if op['channel'] == 'нал':
            sb_moskvich_grouped[name]['nal'] += op['amount']
        else:
            sb_moskvich_grouped[name]['beznal'] += op['amount']

print(f"\nСБ Москвич сгруппированные: {dict(sb_moskvich_grouped)}")

# Группируем СБ из Аноры
sb_anora_grouped = defaultdict(lambda: {'nal': 0, 'beznal': 0})
for op in ops_a:
    if op['code'] == 'СБ':
        name = op['name']
        if sb_merges_anora and name in sb_merges_anora:
            name = sb_merges_anora[name]
        
        if op['channel'] == 'нал':
            sb_anora_grouped[name]['nal'] += op['amount']
        else:
            sb_anora_grouped[name]['beznal'] += op['amount']

print(f"СБ Анора сгруппированные: {dict(sb_anora_grouped)}")

# 3. Добавляем СБ
for name, amounts in sb_moskvich_grouped.items():
    if ('СБ', name) not in processed:
        print(f"Добавляем из Москвича: {name}")
        if amounts['nal'] > 0:
            merged_ops.append({'code': 'СБ', 'name': name, 'channel': 'нал', 'amount': amounts['nal']})
        if amounts['beznal'] > 0:
            merged_ops.append({'code': 'СБ', 'name': name, 'channel': 'безнал', 'amount': amounts['beznal']})
        processed.add(('СБ', name))

for name, amounts in sb_anora_grouped.items():
    if ('СБ', name) not in processed:
        print(f"Добавляем из Аноры: {name}")
        if amounts['nal'] > 0:
            merged_ops.append({'code': 'СБ', 'name': name, 'channel': 'нал', 'amount': amounts['nal']})
        if amounts['beznal'] > 0:
            merged_ops.append({'code': 'СБ', 'name': name, 'channel': 'безнал', 'amount': amounts['beznal']})
        processed.add(('СБ', name))

print(f"\nИТОГО merged_ops: {len(merged_ops)} операций")
print(f"СБ операций: {len([op for op in merged_ops if op['code'] == 'СБ'])}")
print("\nУникальные СБ:")
unique_sb = set([op['name'] for op in merged_ops if op['code'] == 'СБ'])
for name in sorted(unique_sb):
    print(f"  - {name}")

