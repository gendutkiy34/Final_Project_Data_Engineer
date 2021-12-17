CREATE TABLE case_covid (
	case_id text NULL,
	status_name text NULL,
	status_detail text NULL
);
CREATE TABLE public.district (
	kode_kab text NULL,
	kode_prov text NULL,
	nama_kab text NULL
);
CREATE TABLE public.province (
	kode_prov text NULL,
	nama_prov text NULL
);
CREATE TABLE public.staging_closecontact (
	tanggal timestamp NULL,
	kode_prov text NULL,
	kode_kab text NULL,
	closecontact_discarded int8 NULL,
	closecontact_meninggal int8 NULL,
	closecontact_dikarantina int8 NULL
);
CREATE TABLE public.staging_confirmation (
	tanggal timestamp NULL,
	kode_prov text NULL,
	kode_kab text NULL,
	confirmation_sembuh int8 NULL,
	confirmation_meninggal int8 NULL
);
CREATE TABLE public.staging_probable (
	tanggal timestamp NULL,
	kode_prov text NULL,
	kode_kab text NULL,
	probable_diisolasi int8 NULL,
	probable_discarded int8 NULL,
	probable_meninggal int8 NULL
);
CREATE TABLE public.staging_suspect (
	tanggal timestamp NULL,
	kode_prov text NULL,
	kode_kab text NULL,
	suspect_diisolasi int8 NULL,
	suspect_discarded int8 NULL,
	suspect_meninggal int8 NULL
);
CREATE TABLE public.daily_province (
	id serial4 NOT NULL,
	province_id bpchar(5) NOT NULL,
	case_id bpchar(5) NOT NULL,
	"date" date NULL,
	total int4 NULL,
	CONSTRAINT daily_province_pkey PRIMARY KEY (id)
);
CREATE TABLE public.monthly_province (
	id serial4 NOT NULL,
	province_id bpchar(5) NOT NULL,
	case_id bpchar(5) NOT NULL,
	mont varchar(10) NULL,
	total int4 NULL,
	CONSTRAINT monthly_province_pkey PRIMARY KEY (id)
);
CREATE TABLE public.yearly_province (
	id serial4 NOT NULL,
	province_id bpchar(5) NOT NULL,
	case_id bpchar(5) NOT NULL,
	"year" varchar(4) NULL,
	total int4 NULL,
	CONSTRAINT yearly_province_pkey PRIMARY KEY (id)
);
CREATE TABLE public.monthly_district (
	id serial4 NOT NULL,
	district_id bpchar(8) NOT NULL,
	case_id bpchar(5) NOT NULL,
	mont varchar(10) NULL,
	total int4 NULL,
	CONSTRAINT monthly_distric_pkey PRIMARY KEY (id)
);
CREATE TABLE public.yearly_district (
	id serial4 NOT NULL,
	district_id bpchar(8) NOT NULL,
	case_id bpchar(5) NOT NULL,
	"year" varchar(5) NULL,
	total int4 NULL,
	CONSTRAINT yearly_distric_pkey PRIMARY KEY (id)
);