SELECT a.kode_prov,a.case_id,DATE_FORMAT(tanggal,'%Y-%m') as mon,sum(a.suspect_diisolasi) as total
from
(select kode_prov,tanggal,suspect_diisolasi
      ,(select case_id from case_covid where status_detail='suspect_diisolasi') as case_id
from temp_raw_covid) a
group by a.kode_prov,a.case_id,DATE_FORMAT(tanggal,'%Y-%m')