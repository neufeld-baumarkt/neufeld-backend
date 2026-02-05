 SELECT wb.id,
    wb.filiale,
    wb.jahr,
    wb.kw,
    wb.freigegeben,
    wb.umsatz_vorwoche_brutto,
    wr.prozentsatz AS prozentsatz_effektiv,
    wr.mwst_faktor AS mwst_faktor_effektiv,
        CASE
            WHEN wb.umsatz_vorwoche_brutto IS NULL OR wr.mwst_faktor IS NULL OR wr.mwst_faktor = 0::numeric OR wr.prozentsatz IS NULL THEN NULL::numeric
            ELSE round(wb.umsatz_vorwoche_brutto / wr.mwst_faktor * wr.prozentsatz, 2)
        END AS budget_freigegeben_netto,
    COALESCE(sum(
        CASE
            WHEN b.status <> 'storniert'::text THEN b.betrag
            ELSE 0::numeric
        END), 0::numeric)::numeric(14,2) AS verbraucht,
        CASE
            WHEN wb.umsatz_vorwoche_brutto IS NULL OR wr.mwst_faktor IS NULL OR wr.mwst_faktor = 0::numeric OR wr.prozentsatz IS NULL THEN NULL::numeric
            ELSE round(wb.umsatz_vorwoche_brutto / wr.mwst_faktor * wr.prozentsatz - COALESCE(sum(
            CASE
                WHEN b.status <> 'storniert'::text THEN b.betrag
                ELSE 0::numeric
            END), 0::numeric), 2)
        END AS rest_netto,
    count(*) FILTER (WHERE b.status = 'offen'::text) AS offene_buchungen
   FROM budget.week_budgets wb
     LEFT JOIN budget.week_rules wr ON wr.jahr = wb.jahr AND wr.kw = wb.kw
     LEFT JOIN budget.bookings b ON b.week_budget_id = wb.id
  GROUP BY wb.id, wb.filiale, wb.jahr, wb.kw, wb.freigegeben, wb.umsatz_vorwoche_brutto, wr.prozentsatz, wr.mwst_faktor;
