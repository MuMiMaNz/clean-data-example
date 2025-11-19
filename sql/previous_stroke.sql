DROP table if exists hdctemp.mumi_previous_stroke;
CREATE table hdctemp.mumi_previous_stroke
AS SELECT c.cid, t2.hospcode, t2.pid, t2.date_serv, t2.diagcode
FROM
  (
  SELECT    t1.hospcode, t1.pid, t1.date_serv, t1.diagcode
  FROM
    (
    SELECT    a.hospcode, a.pid, a.date_serv, a.diagcode
    FROM      hdc.diagnosis_opd a
    WHERE     regexp_replace(trim(upper(a.diagcode)),"\\.","") like "I6%"

      UNION ALL

    SELECT    b.hospcode, b.pid, date(b.datetime_admit) as date_serv, b.diagcode
    FROM      hdc.diagnosis_ipd b
    WHERE     regexp_replace(trim(upper(b.diagcode)),"\\.","") like "I6%"
    )t1
  LEFT JOIN hdc.chospital x ON t1.hospcode = x.hoscode
  WHERE   x.provcode = "19"
          AND x.hostype in ("03","05","06","07","08","11","12","13","15","16")
  )t2
LEFT JOIN hdc.person c on c.hospcode = t2.hospcode AND c.pid = t2.pid