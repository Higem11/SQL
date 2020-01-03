WITH lag as  --расчет разницы времени между 2 последовательными событиями (по каждому пользователю)
(
    select *, happened_at - lag(happened_at) over (partition by user_id order by happened_at) lag
    from test.vimbox_pages
),

is_session_end as  -- если разница по времени по одному пользователю > 1 часа => ставим отметку, что это конец сессии
(
    select 
        lag.*,
        sum(case when lag.lag > '1 hour'::interval then 1 else 0 end) over 
        (partition by user_id order by happened_at rows between unbounded preceding and current row) is_session_end
    from lag
    order by user_id
),

unique_sessions as  -- нумеруем каждую сессию индивидуально и изменяем имена нужных действий на числовое (для последующей фильтрации)
(
    select *,
        dense_rank() over (order by user_id, is_session_end) unique_session_id,
        case when page = 'rooms.homework-showcase' then '1'
        when page = 'rooms.view.step.content' then '2'
        when page = 'rooms.lesson.rev.step.content' then '3'
        else '0' end action_type 
    from is_session_end
),

min_max_time as  --ищем минимальное время по 1 действию и максимальное по 3 действию (внутри каждой уникальной сессии)
(
    select *,
        case when action_type = '1' then min(happened_at) over (partition by unique_session_id, page) end min_1_time,
        case when action_type = '3' then max(happened_at) over (partition by unique_session_id, page) end max_3_time
    from unique_sessions
),

is_2_OK as  --отмечаем через "1" любое 2 действие, которое по времени лежут между 1 и 3 действиями
(
    select *,
        case when action_type = '2' then case when to_char(happened_at, 'yyyy-mm-dd hh24:mi:ss') >= to_char (min(min_1_time) over (partition by unique_session_id), 'yyyy-mm-dd hh24:mi:ss')
        and to_char(happened_at, 'yyyy-mm-dd hh24:mi:ss') <= to_char (max(max_3_time) over (partition by unique_session_id), 'yyyy-mm-dd hh24:mi:ss') then 1 end
        end secondaction_verify
    from min_max_time
),

is_session_OK as --отбираем те сессии, где есть хотя бы одно 2ое действие, лежащее между 1 и 3 действиями
(
    select unique_session_id, user_id, happened_at,
        case when max(secondaction_verify) over (partition by unique_session_id) = 1 then 'OK' else 'not OK' end OK
    from is_2_OK
),

OK_sessions as --ищем начала, конец и длительности каждой из сессий, разбиваем по времени дня
(
    select unique_session_id, user_id,
        min(happened_at) session_start,
        max(happened_at) + '1 hour' :: interval sessiond_end,
        max(happened_at) + '1 hour' :: interval - min(happened_at) session_time,
        extract (HOUR from session_time) * 60 + extract (MINUTES from session_time) session_time_int,
        CASE
        WHEN date_part ('hour', session_start) >= '6' and date_part ('hour', session_start) < '12' then '1 - утро'
        WHEN date_part ('hour', session_start) >= '12' and date_part ('hour', session_start) < '18' then '2 - день'
        WHEN date_part ('hour', session_start) >= '18' and date_part ('hour', session_start) <= '23' then '3 - вечер'
        WHEN date_part ('hour', session_start) >= '00' and date_part ('hour', session_start) < '6' then '4 - ночь'
        end timeoftheday
    from is_session_OK
    where is_session_OK.OK = 'OK'
    group by 1, 2
)

select --формируем вывод
    timeoftheday "Время суток",
    count(unique_session_id) "Количество сессий",
    min(session_time_int) "Min длительность сессии",
    max(session_time_int) "Max длительность сессии",
    avg(session_time_int) :: integer "Средняя длительность сессии"
from OK_sessions
group by 1