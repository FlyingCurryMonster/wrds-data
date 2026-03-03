SELECT
    optionm_all.zerocd.date,
    optionm_all.zerocd.days,
    optionm_all.zerocd.rate

FROM optionm_all.zerocd

WHERE optionm_all.zerocd.date BETWEEN
    '2000-01-01'::date AND '2023-08-31'::date
