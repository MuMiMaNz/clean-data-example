DROP TABLE IF EXISTS hdctemp.mumi_proj_drug;

CREATE TABLE hdctemp.mumi_proj_drug AS 

SELECT  
    t1.source,
    p.cid,
    t1.hospcode,
    t1.pid,
    t1.seq,
    t1.date_serv,
    t1.clinic,
    t1.an,
    t1.datetime_admit,
    t1.wardstay,
    t1.typedrug,
    collect_list(t1.didstd) as didstds,
    collect_list(t1.dname) as dnames
FROM (
    SELECT 
        'OPD' AS source,
        d.hospcode,
        d.pid,
        d.seq,
        d.date_serv,
        d.clinic,
        d.didstd,
        d.dname,
        NULL AS an,
        NULL AS datetime_admit,
        NULL AS wardstay,
        NULL AS typedrug
    FROM hdc.drug_opd d
    JOIN hdc.person p ON d.hospcode = p.hospcode AND d.pid = p.pid
    JOIN hdc.chospital x ON d.hospcode = x.hoscode
    WHERE x.provcode = '19'
    AND x.hostype IN ('03','05','06','07','08','11','12','13','15','16')
    AND (p.nation = '99' OR p.nation ='099')
    AND p.cid NOT IN ('8b0a44048f58988b486bdd0d245b22a8', '8b5ae3fa71f31415e199e627a27590e3', '2f11ae104be51201b8a4816819574b4b')

    UNION ALL

    SELECT 
        'IPD' AS source,
        d.hospcode,
        d.pid,
        a.seq,
        NULL AS date_serv,
        NULL AS clinic,
        d.didstd,
        d.dname,
        d.an,
        d.datetime_admit,
        d.wardstay,
        d.typedrug
    FROM hdc.drug_ipd d
    JOIN hdc.person p ON d.hospcode = p.hospcode AND d.pid = p.pid
    JOIN hdc.chospital x ON d.hospcode = x.hoscode
    LEFT JOIN hdc.admission a ON d.hospcode = a.hospcode AND d.pid = a.pid AND d.an = a.an
    WHERE x.provcode = '19'
    AND x.hostype IN ('03','05','06','07','08','11','12','13','15','16')
    AND (p.nation = '99' OR p.nation ='099')
    AND p.cid NOT IN ('8b0a44048f58988b486bdd0d245b22a8', '8b5ae3fa71f31415e199e627a27590e3', '2f11ae104be51201b8a4816819574b4b')
) AS t1
JOIN hdc.person p ON t1.hospcode = p.hospcode AND t1.pid = p.pid
JOIN hdc.chospital x ON t1.hospcode = x.hoscode
WHERE x.provcode = '19'
AND x.hostype IN ('03','05','06','07','08','11','12','13','15','16')
AND (p.nation = '99' OR p.nation ='099')
AND p.cid NOT IN ('8b0a44048f58988b486bdd0d245b22a8', '8b5ae3fa71f31415e199e627a27590e3', '2f11ae104be51201b8a4816819574b4b')
GROUP BY t1.source, p.cid, t1.hospcode, t1.pid, t1.seq, t1.date_serv, t1.clinic, t1.an, t1.datetime_admit, t1.wardstay, t1.typedrug
ORDER BY p.cid, t1.seq;