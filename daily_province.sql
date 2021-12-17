/*update 2021-12-17*/
insert into daily_province (province_id,case_id,date,total)
select kode_prov
       ,(select case_id from case_covid where status_detail='closecontact_discarded') as case_id
       ,tanggal 
       ,sum(closecontact_discarded) as total
from staging_closecontact
group by kode_prov,(select case_id from case_covid where status_detail='closecontact_discarded'),tanggal 
union
select kode_prov
       ,(select case_id from case_covid where status_detail='closecontact_dikarantina') as case_id
       ,tanggal 
       ,sum(closecontact_dikarantina) as total
from staging_closecontact
group by kode_prov,(select case_id from case_covid where status_detail='closecontact_dikarantina'),tanggal 
union
select kode_prov
       ,(select case_id from case_covid where status_detail='closecontact_meninggal') as case_id
       ,tanggal 
       ,sum(closecontact_meninggal) as total
from staging_closecontact
group by kode_prov,(select case_id from case_covid where status_detail='closecontact_meninggal'),tanggal 
union	
select kode_prov
       ,(select case_id from case_covid where status_detail='probable_diisolasi') as case_id
       ,tanggal 
       ,sum(probable_diisolasi) as total
from staging_probable
group by kode_prov,(select case_id from case_covid where status_detail='probable_diisolasi'),tanggal 
union
select kode_prov
       ,(select case_id from case_covid where status_detail='probable_discarded') as case_id
       ,tanggal 
       ,sum(probable_discarded) as total
from staging_probable
group by kode_prov,(select case_id from case_covid where status_detail='probable_discarded'),tanggal 
union
select kode_prov
       ,(select case_id from case_covid where status_detail='probable_meninggal') as case_id
       ,tanggal 
       ,sum(probable_meninggal) as total
from staging_probable
group by kode_prov,(select case_id from case_covid where status_detail='probable_meninggal'),tanggal 
union
select kode_prov
       ,(select case_id from case_covid where status_detail='suspect_diisolasi') as case_id
       ,tanggal 
       ,sum(suspect_diisolasi) as total
from staging_suspect
group by kode_prov,(select case_id from case_covid where status_detail='suspect_diisolasi'),tanggal 
union
select kode_prov
       ,(select case_id from case_covid where status_detail='suspect_discarded') as case_id
       ,tanggal 
       ,sum(suspect_discarded) as total
from staging_suspect
group by kode_prov,(select case_id from case_covid where status_detail='suspect_discarded'),tanggal 
union
select kode_prov
       ,(select case_id from case_covid where status_detail='suspect_meninggal') as case_id
       ,tanggal 
       ,sum(suspect_meninggal) as total
from staging_suspect
group by kode_prov,(select case_id from case_covid where status_detail='suspect_meninggal'),tanggal
union
select kode_prov
       ,(select case_id from case_covid where status_detail='confirmation_sembuh') as case_id
       ,tanggal 
       ,sum(confirmation_sembuh) as total
from staging_confirmation
group by kode_prov,(select case_id from case_covid where status_detail='confirmation_sembuh'),tanggal
union
select kode_prov
       ,(select case_id from case_covid where status_detail='confirmation_meninggal') as case_id
       ,tanggal 
       ,sum(confirmation_meninggal) as total
from staging_confirmation
group by kode_prov,(select case_id from case_covid where status_detail='confirmation_meninggal'),tanggal;