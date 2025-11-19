DROP TABLE IF EXISTS hdctemp.mumi_proj_bp_multi_dx;

CREATE TABLE hdctemp.mumi_proj_bp_multi_dx AS 
SELECT  
    main.cid, main.sex, main.birth, main.education, main.occupation_new, main.typearea, main.instype, 
    main.hospcode, main.pid, main.seq, main.date_serv, main.sbp, main.dbp, 
    collect_list(main.diagcode) as diagcodes, main.`location`, main.intime, main.CHIEFCOMP, main.TYPEIN, main.TYPEOUT,
    ROW_NUMBER() OVER(PARTITION BY main.cid ORDER BY main.date_serv ASC) AS ranking2
FROM (
    SELECT  
        p.cid, p.sex, p.birth, p.education, p.occupation_new, p.typearea,
        s.instype, s.hospcode, s.pid, s.seq, s.date_serv, s.sbp, s.dbp, d.diagcode, s.`location`, s.intime, s.CHIEFCOMP, s.TYPEIN, s.TYPEOUT
    FROM hdc.service s
    LEFT JOIN hdc.chospital x ON s.hospcode = x.hoscode
    LEFT JOIN hdc.person p ON s.hospcode = p.hospcode AND s.pid = p.pid
    LEFT JOIN (
        SELECT hospcode, pid, seq, REGEXP_REPLACE(TRIM(UPPER(diagcode)), '\\.', '') AS diagcode
        FROM hdc.diagnosis_opd
        UNION ALL
        SELECT i.hospcode, i.pid, a.seq, REGEXP_REPLACE(TRIM(UPPER(i.diagcode)), '\\.', '') AS diagcode
        FROM hdc.diagnosis_ipd i
        LEFT JOIN hdc.admission a ON i.hospcode = a.hospcode AND i.pid = a.pid AND i.an = a.an
    ) d ON s.hospcode = d.hospcode AND s.pid = d.pid AND s.seq = d.seq
    WHERE x.provcode = '19'
    AND x.hostype IN ('03','05','06','07','08','11','12','13','15','16')
    AND (p.nation = '99' OR p.nation ='099')
    AND p.cid NOT IN ('8b0a44048f58988b486bdd0d245b22a8', '8b5ae3fa71f31415e199e627a27590e3', '2f11ae104be51201b8a4816819574b4b')
) AS main
GROUP BY main.cid, main.sex, main.birth, main.education, main.occupation_new, main.typearea, main.instype, 
    main.hospcode, main.pid, main.seq, main.date_serv, main.sbp, main.dbp, main.`location`, main.intime, main.CHIEFCOMP, main.TYPEIN, main.TYPEOUT
ORDER BY main.cid, main.date_serv;
