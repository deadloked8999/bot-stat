-- ============================================
-- МИГРАЦИЯ: Добавление потерянных сотрудников из payments в employees
-- ============================================
-- 
-- Описание:
-- Находит всех сотрудников, которые есть в payments, но отсутствуют в employees
-- Использует ПОСЛЕДНЕЕ имя (из самой поздней даты) для каждого сотрудника
-- Использует ПЕРВУЮ дату выплаты как hired_date
--
-- ВАЖНО: Выполнять только если уверены, что это потерянные сотрудники!
-- Проверьте результат перед применением!
-- ============================================

-- ПРЕДВАРИТЕЛЬНАЯ ПРОВЕРКА: Сколько сотрудников будет добавлено
SELECT 
    COUNT(*) as missing_employees_count
FROM (
    SELECT DISTINCT p.code, p.club
    FROM payments p
    LEFT JOIN employees e ON p.code = e.code AND p.club = e.club
    WHERE e.code IS NULL
);

-- ПРЕДВАРИТЕЛЬНЫЙ ПРОСМОТР: Какие сотрудники будут добавлены
SELECT 
    p.code,
    p.club,
    p.last_name,
    p.first_date,
    p.last_date
FROM (
    SELECT 
        code,
        club,
        -- Берём имя из последней записи (самая поздняя дата)
        (SELECT name FROM payments 
         WHERE code = p2.code AND club = p2.club 
         ORDER BY date DESC LIMIT 1) as last_name,
        -- Берём первую дату как hired_date
        MIN(date) as first_date,
        -- Берём последнюю дату для информации
        MAX(date) as last_date
    FROM payments p2
    GROUP BY code, club
) p
LEFT JOIN employees e ON p.code = e.code AND p.club = e.club
WHERE e.code IS NULL
ORDER BY p.club, p.code;

-- ============================================
-- ОСНОВНАЯ МИГРАЦИЯ
-- ============================================
-- Раскомментируйте следующие строки для выполнения:

/*
INSERT INTO employees (code, club, full_name, hired_date, is_active, created_at)
SELECT 
    p.code,
    p.club,
    p.last_name as full_name,          -- Последнее имя (из самой поздней даты)
    p.first_date as hired_date,         -- Первая дата выплаты
    1 as is_active,
    datetime('now') as created_at
FROM (
    SELECT 
        code,
        club,
        -- Берём имя из последней записи (самая поздняя дата)
        (SELECT name FROM payments 
         WHERE code = p2.code AND club = p2.club 
         ORDER BY date DESC LIMIT 1) as last_name,
        -- Берём первую дату как hired_date
        MIN(date) as first_date
    FROM payments p2
    GROUP BY code, club
) p
LEFT JOIN employees e ON p.code = e.code AND p.club = e.club
WHERE e.code IS NULL;
*/

-- ============================================
-- ПРОВЕРКА РЕЗУЛЬТАТА
-- ============================================
-- После выполнения миграции проверьте:

/*
-- Сколько сотрудников теперь в employees
SELECT COUNT(*) as total_employees FROM employees;

-- Сколько уникальных сотрудников в payments
SELECT COUNT(DISTINCT code || '-' || club) as total_in_payments FROM payments;

-- Остались ли потерянные сотрудники
SELECT COUNT(*) as still_missing
FROM (
    SELECT DISTINCT p.code, p.club
    FROM payments p
    LEFT JOIN employees e ON p.code = e.code AND p.club = e.club
    WHERE e.code IS NULL
);
*/

