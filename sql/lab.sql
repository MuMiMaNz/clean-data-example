DROP TABLE IF EXISTS hdctemp.mumi_proj_lab;
CREATE TABLE hdctemp.mumi_proj_lab

AS SELECT  p.cid,t1.hospcode, t1.pid, t1.date_serv,t1.labtest,t1.labresult,
  ROW_NUMBER() OVER(PARTITION BY p.cid, t1.date_serv ORDER BY t1.date_serv ASC) AS ranking
FROM
 (
 SELECT  *
 FROM  hdc.labfu a
 LEFT JOIN hdc.chospital x on a.hospcode = x.hoscode
 WHERE  x.provcode = "19" AND x.hostype in ("03","05","06","07","08","11","12","13","15","16")
 )t1
LEFT JOIN hdc.person p on t1.hospcode = p.hospcode AND t1.pid = p.pid
WHERE  (p.nation = "99" OR p.nation = "099")
  AND p.cid NOT IN ("8b0a44048f58988b486bdd0d245b22a8","8b5ae3fa71f31415e199e627a27590e3","2f11ae104be51201b8a4816819574b4b")