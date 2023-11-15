CREATE TABLE IF NOT EXISTS vm_delta.layer_registry
(
  schTbl varchar(100),
  active boolean,
  relation varchar(5),
  geom_type varchar(20),
  pkey varchar(30),
  status varchar(12),
  err boolean,
  sup varchar(10),
  sup_ver bigint,
  sup_date timestamp with time zone,
	sup_type varchar(4),
	md_uuid varchar(36),
  extradata json,
  edit_date timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT layer_registry_pk PRIMARY KEY (schTbl)
);
