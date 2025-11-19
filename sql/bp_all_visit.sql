-- SELECT  cid, sex, birth, education, occupation_new, typearea,
    -- instype, hospcode, date_serv, sbp, dbp, ranking2
-- FROM
--  (

drop table if exists hdctemp.mumi_proj_bpall;
create table hdctemp.mumi_proj_bpall
 as SELECT  t2.cid, t2.sex, t2.birth, t2.education, t2.occupation_new, t2.typearea,
    t2.instype, t2.hospcode, t2.pid, t2.seq, t2.date_serv, t2.sbp, t2.dbp,
    row_number() over(partition by cid order by date_serv ASC) as ranking2
 FROM
  (
  SELECT  p.cid, p.sex, p.birth, p.education, p.occupation_new, p.typearea,
     t1.instype, t1.hospcode, t1.pid, t1.seq, t1.date_serv, t1.sbp, t1.dbp,
     row_number() over(partition by p.cid,t1.date_serv,t1.hospcode order by t1.date_serv ASC) as ranking
  FROM
   (
   SELECT  s.hospcode, s.pid, s.seq, s.instype, s.date_serv, s.sbp, s.dbp
   FROM  hdc.service s
    LEFT JOIN hdc.chospital x on s.hospcode = x.hoscode
    WHERE (s.sbp BETWEEN 75 AND 275)
    AND (s.dbp BETWEEN 30 AND 160)
    AND (s.sbp-s.dbp BETWEEN 10 AND 150)
    AND x.provcode = "19"
    AND x.hostype in ("03","05","06","07","08","11","12","13","15","16")
   )as t1
  LEFT JOIN hdc.person p on t1.hospcode = p.hospcode AND t1.pid = p.pid
  WHERE  (p.nation = "99" OR p.nation ="099")
    AND p.cid != "8b0a44048f58988b486bdd0d245b22a8"
    AND p.cid != "8b5ae3fa71f31415e199e627a27590e3"
    AND p.cid != "2f11ae104be51201b8a4816819574b4b"
    AND datediff(t1.date_serv,p.birth)/365 >= 30
  )as t2
 WHERE ranking = 1
--  )as t3
-- WHERE ranking2 in (1,2,3,4,5)