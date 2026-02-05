 SELECT id,
    filiale,
    jahr,
    kw,
    freigegeben,
    umsatz_vorwoche_brutto,
    prozentsatz_effektiv,
    mwst_faktor_effektiv,
    budget_freigegeben_netto,
    verbraucht,
    rest_netto,
    offene_buchungen,
        CASE
            WHEN umsatz_vorwoche_brutto IS NULL OR mwst_faktor_effektiv IS NULL OR mwst_faktor_effektiv = 0::numeric THEN NULL::numeric
            ELSE umsatz_vorwoche_brutto / mwst_faktor_effektiv
        END AS umsatz_vorwoche_netto,
    sum(
        CASE
            WHEN umsatz_vorwoche_brutto IS NULL OR mwst_faktor_effektiv IS NULL OR mwst_faktor_effektiv = 0::numeric THEN 0::numeric
            ELSE umsatz_vorwoche_brutto / mwst_faktor_effektiv
        END) OVER (PARTITION BY filiale, jahr ORDER BY kw ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS umsatz_ytd_netto,
    sum(COALESCE(budget_freigegeben_netto, 0::numeric)) OVER (PARTITION BY filiale, jahr ORDER BY kw ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS budget_ytd_netto,
    sum(COALESCE(verbraucht, 0::numeric)) OVER (PARTITION BY filiale, jahr ORDER BY kw ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS verbraucht_ytd,
    sum(COALESCE(budget_freigegeben_netto, 0::numeric)) OVER (PARTITION BY filiale, jahr ORDER BY kw ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - sum(COALESCE(verbraucht, 0::numeric)) OVER (PARTITION BY filiale, jahr ORDER BY kw ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rest_ytd_netto,
        CASE
            WHEN sum(
            CASE
                WHEN umsatz_vorwoche_brutto IS NULL OR mwst_faktor_effektiv IS NULL OR mwst_faktor_effektiv = 0::numeric THEN 0::numeric
                ELSE umsatz_vorwoche_brutto / mwst_faktor_effektiv
            END) OVER (PARTITION BY filiale, jahr ORDER BY kw ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) = 0::numeric THEN NULL::numeric
            ELSE round(sum(COALESCE(budget_freigegeben_netto, 0::numeric)) OVER (PARTITION BY filiale, jahr ORDER BY kw ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 100::numeric / sum(
            CASE
                WHEN umsatz_vorwoche_brutto IS NULL OR mwst_faktor_effektiv IS NULL OR mwst_faktor_effektiv = 0::numeric THEN 0::numeric
                ELSE umsatz_vorwoche_brutto / mwst_faktor_effektiv
            END) OVER (PARTITION BY filiale, jahr ORDER BY kw ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2)
        END AS budget_satz_ytd_prozent
   FROM budget.v_week_summary_global s;
