insert into yearly_district (district_id,case_id,year,total)
select kode_kab
       ,(select case_id from case_covid where status_detail='closecontact_discarded') as case_id
       ,to_char(tanggal,'YYYY') as tahun
       ,sum(closecontact_discarded) as total
from staging_closecontact
group by kode_kab,(select case_id from case_covid where status_detail='closecontact_discarded'),to_char(tanggal,'YYYY') 
union
select kode_kab
       ,(select case_id from case_covid where status_detail='closecontact_dikarantina') as case_id
       ,to_char(tanggal,'YYYY') as tahun
       ,sum(closecontact_dikarantina) as total
from staging_closecontact
group by kode_kab,(select case_id from case_covid where status_detail='closecontact_dikarantina'),to_char(tanggal,'YYYY') 
union
select kode_kab
       ,(select case_id from case_covid where status_detail='closecontact_meninggal') as case_id
       ,to_char(tanggal,'YYYY') as tahun
       ,sum(closecontact_meninggal_harian) as total
from staging_closecontact
group by kode_kab,(select case_id from case_covid where status_detail='closecontact_meninggal'),to_char(tanggal,'YYYY') 
union	
select kode_kab
       ,(select case_id from case_covid where status_detail='probable_diisolasi') as case_id
       ,to_char(tanggal,'YYYY') as tahun
       ,sum(probable_diisolasi) as total
from staging_probable
group by kode_kab,(select case_id from case_covid where status_detail='probable_diisolasi'),to_char(tanggal,'YYYY') 
union
select kode_kab
       ,(select case_id from case_covid where status_detail='probable_discarded') as case_id
       ,to_char(tanggal,'YYYY') as tahun
       ,sum(probable_discarded) as total
from staging_probable
group by kode_kab,(select case_id from case_covid where status_detail='probable_discarded'),to_char(tanggal,'YYYY') 
union
select kode_kab
       ,(select case_id from case_covid where status_detail='probable_meninggal') as case_id
       ,to_char(tanggal,'YYYY') as tahun
       ,sum(probable_meninggal) as total
from staging_probable
group by kode_kab,(select case_id from case_covid where status_detail='probable_meninggal'),to_char(tanggal,'YYYY') 
union
select kode_kab
       ,(select case_id from case_covid where status_detail='suspect_diisolasi') as case_id
       ,to_char(tanggal,'YYYY') as tahun
       ,sum(suspect_diisolasi) as total
from staging_suspect
group by kode_kab,(select case_id from case_covid where status_detail='suspect_diisolasi'),to_char(tanggal,'YYYY') 
union
select kode_kab
       ,(select case_id from case_covid where status_detail='suspect_discarded') as case_id
       ,to_char(tanggal,'YYYY') as tahun
       ,sum(suspect_discarded) as total
from staging_suspect
group by kode_kab,(select case_id from case_covid where status_detail='suspect_discarded'),to_char(tanggal,'YYYY') 
union
select kode_kab
       ,(select case_id from case_covid where status_detail='suspect_meninggal') as case_id
       ,to_char(tanggal,'YYYY') as tahun
       ,sum(suspect_meninggal_harian) as total
from staging_suspect
group by kode_kab,(select case_id from case_covid where status_detail='suspect_meninggal'),to_char(tanggal,'YYYY')
union
select kode_kab
       ,(select case_id from case_covid where status_detail='confirmation_sembuh') as case_id
       ,to_char(tanggal,'YYYY') as tahun
       ,sum(confirmation_selesai) as total
from staging_confirmation
group by kode_kab,(select case_id from case_covid where status_detail='confirmation_sembuh'),to_char(tanggal,'YYYY')
union
select kode_kab
       ,(select case_id from case_covid where status_detail='confirmation_meninggal') as case_id
       ,to_char(tanggal,'YYYY') as tahun
       ,sum(confirmation_meninggal) as total
from staging_confirmation
group by kode_kab,(select case_id from case_covid where status_detail='confirmation_meninggal'),to_char(tanggal,'YYYY');