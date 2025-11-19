drop table if exists hdctemp.mumi_bp_dx_imc;

create table hdctemp.mumi_bp_dx_imc
as 

SELECT  t2.cid, t2.sex, t2.birth, t2.education, t2.occupation_new, t2.typearea, t2.instype, 
        t2.hospcode, t2.pid, t2.seq, t2.date_serv, t2.sbp, t2.dbp, t2.diagcode,
        row_number() over(partition by cid order by date_serv ASC) as ranking2

FROM (
    SELECT  p.cid, p.sex, p.birth, p.education, p.occupation_new, p.typearea,
            t1.instype, t1.hospcode, t1.pid, t1.seq, t1.date_serv, t1.sbp, t1.dbp, t1.diagcode,
            row_number() over(partition by p.cid,t1.date_serv,t1.hospcode order by t1.date_serv ASC) as ranking
    FROM (
        SELECT  s.hospcode, s.pid, s.seq, s.instype, s.date_serv, s.sbp, s.dbp, d1.diagcode
        FROM  hdc.service s
        LEFT JOIN hdc.chospital x on s.hospcode = x.hoscode
        LEFT JOIN (
            SELECT  hospcode, pid, seq, regexp_replace(trim(upper(diagcode)),"\\.","") as diagcode
            FROM    hdc.diagnosis_opd
            UNION ALL
            
            SELECT tb3.hospcode, tb3.pid, tb3.seq, tb3.diagcode
            FROM
            (
            SELECT      tb2.hospcode, tb2.pid, c.seq, tb2.diagcode
            FROM
                (
                SELECT  i.hospcode, i.pid, i.an, regexp_replace(trim(upper(i.diagcode)),"\\.","") as diagcode
                FROM    hdc.diagnosis_ipd i
                )tb2
            LEFT JOIN   hdc.admission c ON tb2.hospcode = c.hospcode AND tb2.pid = c.pid AND tb2.an = c.an
            )tb3
        
        
        ) d1 ON s.hospcode = d1.hospcode AND s.pid = d1.pid AND s.seq = d1.seq
    ) as t1
    LEFT JOIN hdc.person p on t1.hospcode = p.hospcode AND t1.pid = p.pid
    WHERE p.cid IN (
        'df524c9dc0d1423023853803bbc02d5a',
    '7f84340252c39f988c08ae83da5f1ba1',
    '952dbc3573c027765e2323ee96a32b22',
    '83f07e932d9f3e8f3c8433546b8b04db',
    '0e898e4c4d8787356cd971205d368153',
    '360d541242e07d7fef4bf6e8907ab1cf',
    'def9c058a6cba9594d908132e06110d0',
    'b5e19267d8ff85e59ff7bb6e397b90b3',
    'c362ccb5ff213e16b1cb145db4420cfc',
    '077a57c2f80e97e40639282a6664720e',
    'c4cb9b5a69cfae47bbff57b3baaccb0d',
    '156d856b491b11cbd8191d8ab0698842',
    '9f3c38911fff30f13599e103374da16c',
    '642c333571f14a516d411eb937899044',
    '5313a4e3af384074364c0750ec32062e',
    '9ea4b9b79b312b1976b55a6c8f08c2a3',
    '0c30425af1da0f189ca8f53fe0e48831',
    'cc937ea7240eddcf32985d57c209a048',
    'c82f88c507ef3a30ee7894e1f4d676f9',
    'cfc5c0ef5fd310be0ce8362224c0ff11',
    '546213ee61e36b55a041c778bb0b1cb9',
    'd496f15b655557f469365ce94ae9a266',
    '6cab12e67a3e07a32bb692db4a90e424',
    '7c6424c48eaa7d5c94d00e63a1ff53ac',
    '69dc2b81c769d0151ff05a969135a641',
    '8ee8bf0e985e27b3edcaeea372ca3b8a'
    )
) as t2
WHERE ranking = 1;
