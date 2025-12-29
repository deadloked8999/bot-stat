-- Проверка имен в operations за период (замените даты на ваши)
SELECT DISTINCT code, name_snapshot as name, club
FROM operations
WHERE club = 'Москвич'  -- или 'Анора'
  AND date BETWEEN '2024-12-14' AND '2024-12-20'
  AND code LIKE 'Д%'
ORDER BY code;

-- Проверка имен в stylist_expenses за тот же период
SELECT code, name, amount, period_from, period_to, club
FROM stylist_expenses
WHERE club = 'Москвич'  -- или 'Анора'
  AND period_from = '2024-12-14'
  AND period_to = '2024-12-20'
ORDER BY code;

-- Сравнение: какие коды есть в стилистах, но имена не совпадают
SELECT 
    s.code as stylist_code,
    s.name as stylist_name,
    o.name_snapshot as operation_name,
    s.amount
FROM stylist_expenses s
LEFT JOIN operations o 
    ON s.code = o.code 
    AND s.club = o.club
    AND o.date BETWEEN s.period_from AND s.period_to
WHERE s.club = 'Москвич'  -- или 'Анора'
  AND s.period_from = '2024-12-14'
  AND s.period_to = '2024-12-20'
GROUP BY s.code, s.name, s.amount;

