drop table if exists hdctemp.mumi_proj_firstdx;
create table hdctemp.mumi_proj_firstdx

AS SELECT * FROM
(
SELECT  e.cid, tb4.hospcode, tb4.pid, tb4.seq, tb4.date_serv, tb4.sbp, tb4.dbp, tb4.diagcode,
     row_number() over(partition by cid,diagcode order by date_serv) as ranking

FROM
    (
    SELECT      tb1.hospcode, tb1.pid, tb1.seq, tb1.diagcode, b.date_serv, b.sbp, b.dbp
    FROM
        (
            SELECT  a.hospcode, a.pid, a.seq, regexp_replace(trim(upper(a.diagcode)),"\\.","") as diagcode
            FROM    hdc.diagnosis_opd a
            WHERE   diagcode like "I1%"
        )tb1
    LEFT JOIN   hdc.service b ON tb1.hospcode = b.hospcode AND tb1.pid = b.pid AND tb1.seq = b.seq
    
    UNION ALL
    
    SELECT      tb3.hospcode, tb3.pid, tb3.seq, tb3.diagcode, d.date_serv, d.sbp, d.dbp
    FROM
        (
        SELECT      tb2.hospcode, tb2.pid, c.seq, tb2.an, tb2.diagcode
        FROM
            (
                SELECT  i.hospcode, i.pid, i.an, regexp_replace(trim(upper(i.diagcode)),"\\.","") as diagcode
                FROM    hdc.diagnosis_ipd i
                WHERE   diagcode like "I1%"
            )tb2
        LEFT JOIN   hdc.admission c ON tb2.hospcode = c.hospcode AND tb2.pid = c.pid AND tb2.an = c.an
        )tb3
    LEFT JOIN   hdc.service d ON tb3.hospcode = d.hospcode AND tb3.pid = d.pid AND tb3.seq = d.seq
    
    UNION ALL
    
    SELECT  tb5.hospcode, tb5.pid, x.seq, tb5.chronic, tb5.date_serv, x.sbp, x.dbp
    FROM
        (
            SELECT  c.hospcode, c.pid, regexp_replace(trim(upper(c.chronic)),"\\.","") as chronic, c.date_diag as date_serv
            FROM    hdc.chronic c
            WHERE   chronic like "I1%"
        )tb5
    LEFT JOIN hdc.service x on tb5.hospcode = x.hospcode AND tb5.pid = x.pid AND tb5.date_serv = x.date_serv

    )tb4
LEFT JOIN   hdc.person e ON tb4.hospcode = e.hospcode AND tb4.pid = e.pid
WHERE date_serv IS NOT NULL
)tb5

WHERE ranking = 1