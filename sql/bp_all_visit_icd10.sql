DROP TABLE IF EXISTS hdctemp.mumi_proj_bpall_dx;

CREATE TABLE hdctemp.mumi_proj_bpall_dx AS 
SELECT  
    t2.cid, t2.sex, t2.birth, t2.education, t2.occupation_new, t2.typearea, t2.instype, 
    t2.hospcode, t2.pid, t2.seq, t2.date_serv, t2.sbp, t2.dbp, t2.diagcode,
    ROW_NUMBER() OVER(PARTITION BY cid ORDER BY date_serv ASC) AS ranking2
FROM (
    SELECT  
        p.cid, p.sex, p.birth, p.education, p.occupation_new, p.typearea,
        t1.instype, t1.hospcode, t1.pid, t1.seq, t1.date_serv, t1.sbp, t1.dbp, t1.diagcode,
        ROW_NUMBER() OVER(PARTITION BY p.cid, t1.date_serv, t1.hospcode ORDER BY t1.date_serv ASC) AS ranking
    FROM (
        SELECT  
            s.hospcode, s.pid, s.seq, s.instype, s.date_serv, s.sbp, s.dbp, d1.diagcode
        FROM hdc.service s
        LEFT JOIN hdc.chospital x ON s.hospcode = x.hoscode
        LEFT JOIN (
            SELECT hospcode, pid, seq, REGEXP_REPLACE(TRIM(UPPER(diagcode)), '\\.', '') AS diagcode
            FROM hdc.diagnosis_opd
            UNION ALL
            SELECT tb3.hospcode, tb3.pid, tb3.seq, tb3.diagcode
            FROM (
                SELECT tb2.hospcode, tb2.pid, c.seq, tb2.diagcode
                FROM (
                    SELECT i.hospcode, i.pid, i.an, REGEXP_REPLACE(TRIM(UPPER(i.diagcode)), '\\.', '') AS diagcode
                    FROM hdc.diagnosis_ipd i
                ) tb2
                LEFT JOIN hdc.admission c ON tb2.hospcode = c.hospcode AND tb2.pid = c.pid AND tb2.an = c.an
            ) tb3
        ) d1 ON s.hospcode = d1.hospcode AND s.pid = d1.pid AND s.seq = d1.seq
        WHERE (s.sbp BETWEEN 75 AND 275)
        AND (s.dbp BETWEEN 30 AND 160)
        AND (s.sbp-s.dbp BETWEEN 10 AND 150)
        AND x.provcode = '19'
        AND x.hostype IN ('03','05','06','07','08','11','12','13','15','16')
    ) AS t1
    LEFT JOIN hdc.person p ON t1.hospcode = p.hospcode AND t1.pid = p.pid
    WHERE (p.nation = '99' OR p.nation ='099')
    AND p.cid NOT IN ('8b0a44048f58988b486bdd0d245b22a8', '8b5ae3fa71f31415e199e627a27590e3', '2f11ae104be51201b8a4816819574b4b')
    
) AS t2
WHERE ranking = 1;
